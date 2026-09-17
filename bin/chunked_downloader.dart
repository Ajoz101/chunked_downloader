import 'dart:async';
import 'dart:io';
import 'dart:math' as math;
import 'package:http/http.dart' as http;
import 'package:path/path.dart' as p;

class ChunkProgress {
  int downloadedBytes = 0;
  int totalBytes = 0;
  bool isCompleted = false;
}

/// Simple counting semaphore so we don't blow past a reasonable number
/// of concurrent connections to the same host.
class Semaphore {
  Semaphore(this.maxCount) : _current = maxCount;

  final int maxCount;
  int _current;
  final Queue<Completer<void>> _waiting = Queue<Completer<void>>();

  Future<void> acquire() {
    if (_current > 0) {
      _current--;
      return Future.value();
    }
    final completer = Completer<void>();
    _waiting.add(completer);
    return completer.future;
  }

  void release() {
    if (_waiting.isNotEmpty) {
      _waiting.removeFirst().complete();
    } else {
      _current++;
    }
  }

  Future<T> withResource<T>(Future<T> Function() task) async {
    await acquire();
    try {
      return await task();
    } finally {
      release();
    }
  }
}

// Minimal Queue so we don't need dart:collection imported twice / extra deps.
class Queue<T> {
  final List<T> _items = [];
  bool get isNotEmpty => _items.isNotEmpty;
  void add(T item) => _items.add(item);
  T removeFirst() => _items.removeAt(0);
}

void main(List<String> arg) async {
  if (arg.length < 2) {
    print('Usage: dart main.dart <URL> <OutputFile> [NumChunks] [MaxConcurrency]');
    return;
  }

  final url = arg[0];
  final outputPath = arg[1];
  final requestedChunks = arg.length >= 3 ? (int.tryParse(arg[2]) ?? 4) : 4;
  final maxConcurrency = arg.length >= 4
      ? (int.tryParse(arg[3]) ?? math.min(requestedChunks, 8))
      : math.min(requestedChunks, 8);

  final client = http.Client();
  StreamSubscription<ProcessSignal>? sigintSub;
  Timer? uiTimer;

  try {
    // 1. Probe the server: content-length AND whether it actually honors ranges.
    int contentLength = 0;
    bool serverSupportsRanges = false;

    try {
      final headResp = await client.head(
        Uri.parse(url),
        headers: {'User-Agent': 'Dart/3.0 (CLI Downloader)'},
      );
      contentLength = int.tryParse(headResp.headers['content-length'] ?? '') ?? 0;
      serverSupportsRanges =
          (headResp.headers['accept-ranges']?.toLowerCase() == 'bytes');
    } catch (e) {
      print('HEAD request failed: $e');
      return;
    }

    if (contentLength == 0) {
      print('Could not determine file size (no Content-Length header). '
          'The server may use chunked transfer encoding; falling back to a '
          'single-stream download without a known total size.');
      await _singleStreamDownload(client, url, outputPath);
      return;
    }

    if (!serverSupportsRanges) {
      print('Server did not advertise "Accept-Ranges: bytes" — '
          'downloading as a single stream instead of $requestedChunks chunks.');
      await _singleStreamDownload(client, url, outputPath, expectedLength: contentLength);
      return;
    }

    // Double-check with a real ranged probe: some servers lie about Accept-Ranges.
    final probeOk = await _probeRangeSupport(client, url, contentLength);
    if (!probeOk) {
      print('Server advertised range support but ignored a Range request '
          '(returned 200 instead of 206) — falling back to single-stream download.');
      await _singleStreamDownload(client, url, outputPath, expectedLength: contentLength);
      return;
    }

    final numChunks = math.max(1, requestedChunks);
    final chunkSize = (contentLength / numChunks).ceil();
    final tmpDir = Directory('chunks_${p.basename(outputPath)}');
    if (!tmpDir.existsSync()) tmpDir.createSync(recursive: true);

    final Map<int, ChunkProgress> progressMap = {};
    final Stopwatch stopwatch = Stopwatch()..start();

    for (int i = 0; i < numChunks; i++) {
      final start = i * chunkSize;
      final end = (i == numChunks - 1) ? contentLength - 1 : (start + chunkSize - 1);
      progressMap[i] = ChunkProgress()..totalBytes = (end - start + 1);
    }

    // Clean up gracefully on Ctrl+C: stop the UI timer and leave partial
    // .part files in place (they're resumable on next run) rather than
    // leaving the terminal in a torn state.
    sigintSub = ProcessSignal.sigint.watch().listen((_) {
      uiTimer?.cancel();
      stdout.writeln('\n⏸  Interrupted — partial chunks kept in ${tmpDir.path} for resume.');
      exit(130);
    });

    uiTimer = Timer.periodic(const Duration(milliseconds: 200), (_) {
      renderProgressBar(numChunks, progressMap, contentLength, stopwatch.elapsed);
    });

    final semaphore = Semaphore(math.max(1, maxConcurrency));
    final List<Future<void>> downloadTasks = [];
    for (int i = 0; i < numChunks; i++) {
      final start = i * chunkSize;
      final end = (i == numChunks - 1) ? contentLength - 1 : (start + chunkSize - 1);
      final chunkPath = p.join(tmpDir.path, 'chunk_$i.part');

      downloadTasks.add(semaphore.withResource(
        () => downloadChunk(client, url, start, end, chunkPath, progressMap[i]!),
      ));
    }

    try {
      await Future.wait(downloadTasks);
    } catch (e) {
      uiTimer.cancel();
      print('\n❌ Download failed: $e');
      print('   Partial chunks were kept in ${tmpDir.path} — rerun the same '
          'command to resume.');
      return;
    }

    uiTimer.cancel();
    stopwatch.stop();

    renderProgressBar(numChunks, progressMap, contentLength, stopwatch.elapsed, forceFinal: true);

    // 4. Merge chunks
    stdout.writeln('\nMerging chunks into final file...');
    final outputFile = File(outputPath);
    final sink = outputFile.openWrite();

    try {
      for (int i = 0; i < numChunks; i++) {
        final chunkFile = File(p.join(tmpDir.path, 'chunk_$i.part'));
        if (!chunkFile.existsSync()) {
          throw Exception('Missing chunk $i file.');
        }
        await sink.addStream(chunkFile.openRead());
      }
    } finally {
      await sink.close();
    }

    // Sanity check: merged file size should match what the server reported.
    final finalSize = await outputFile.length();
    if (finalSize != contentLength) {
      stdout.writeln('⚠️  Warning: merged file is $finalSize bytes, expected '
          '$contentLength bytes. The file may be corrupt.');
    }

    tmpDir.deleteSync(recursive: true);
    stdout.writeln('✅ Download finished: $outputPath');
  } finally {
    await sigintSub?.cancel();
    client.close();
  }
}

/// Sends a small ranged GET (first byte only) to confirm the server actually
/// responds with 206 Partial Content rather than silently sending the whole
/// file back with a 200.
Future<bool> _probeRangeSupport(http.Client client, String url, int contentLength) async {
  try {
    final request = http.Request('GET', Uri.parse(url));
    request.headers['Range'] = 'bytes=0-0';
    request.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)';
    final response = await client.send(request);
    // Drain the body so the connection can be reused/closed cleanly.
    await response.stream.drain();
    return response.statusCode == 206;
  } catch (_) {
    return false;
  }
}

/// Fallback path for servers that don't support (or lie about) byte ranges.
/// Downloads the whole file in one stream with basic retry, and resumes
/// from the last written byte on retry when the server cooperates.
Future<void> _singleStreamDownload(
  http.Client client,
  String url,
  String outputPath, {
  int? expectedLength,
  int maxRetries = 5,
}) async {
  final file = File(outputPath);
  int retries = 0;
  final stopwatch = Stopwatch()..start();

  while (true) {
    final existingBytes = file.existsSync() ? await file.length() : 0;
    if (expectedLength != null && existingBytes >= expectedLength) {
      stdout.writeln('✅ Download finished: $outputPath');
      return;
    }

    try {
      final request = http.Request('GET', Uri.parse(url));
      request.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)';
      if (existingBytes > 0) {
        request.headers['Range'] = 'bytes=$existingBytes-';
      }
      final response = await client.send(request);

      if (response.statusCode != 200 && response.statusCode != 206) {
        throw HttpException('Unexpected status code: ${response.statusCode}');
      }

      final sink = file.openWrite(
        mode: existingBytes > 0 && response.statusCode == 206
            ? FileMode.append
            : FileMode.write,
      );
      int downloaded = existingBytes > 0 && response.statusCode == 206 ? existingBytes : 0;

      await for (final chunk in response.stream) {
        sink.add(chunk);
        downloaded += chunk.length;
        if (stopwatch.elapsedMilliseconds > 200) {
          stopwatch.reset();
          final mb = (downloaded / (1024 * 1024)).toStringAsFixed(1);
          final totalMb = expectedLength != null
              ? (expectedLength / (1024 * 1024)).toStringAsFixed(1)
              : '?';
          stdout.write('\rDownloaded: $mb / $totalMb MB   ');
        }
      }

      await sink.flush();
      await sink.close();
      stdout.writeln('\n✅ Download finished: $outputPath');
      return;
    } catch (e) {
      retries++;
      if (retries > maxRetries) rethrow;
      await Future.delayed(Duration(seconds: 2 * retries));
    }
  }
}

Future<void> downloadChunk(
  http.Client client,
  String url,
  int start,
  int end,
  String filePath,
  ChunkProgress progress, {
  int maxRetries = 5,
}) async {
  final file = File(filePath);
  int retries = 0;

  while (retries <= maxRetries) {
    int existingBytes = file.existsSync() ? file.lengthSync() : 0;
    progress.downloadedBytes = existingBytes;

    if (existingBytes >= progress.totalBytes) {
      progress.downloadedBytes = progress.totalBytes;
      progress.isCompleted = true;
      return;
    }

    int currentStart = start + existingBytes;

    try {
      final request = http.Request('GET', Uri.parse(url));
      request.headers['Range'] = 'bytes=$currentStart-$end';
      request.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)';

      final response = await client.send(request);

      if (response.statusCode == 206) {
        final sink = file.openWrite(mode: FileMode.append);

        await for (final chunk in response.stream) {
          sink.add(chunk);
          progress.downloadedBytes += chunk.length;
        }

        await sink.flush();
        await sink.close();
        progress.isCompleted = true;
        return;
      } else if (response.statusCode == 200) {
        // Server ignored our Range header and is sending the full body —
        // writing this into a chunk file would corrupt the merge, so abort
        // loudly instead of silently producing a broken download.
        await response.stream.drain();
        throw Exception(
            'Server returned 200 instead of 206 for a ranged request on chunk '
            '(bytes=$currentStart-$end). Aborting to avoid a corrupt merge — '
            'retry with NumChunks=1.');
      } else {
        throw HttpException('Unexpected status code: ${response.statusCode}');
      }
    } catch (e) {
      retries++;
      if (retries > maxRetries) rethrow;
      await Future.delayed(Duration(seconds: 2 * retries));
    }
  }
}

void renderProgressBar(
  int numChunks,
  Map<int, ChunkProgress> progressMap,
  int totalContentLength,
  Duration elapsed, {
  bool forceFinal = false,
}) {
  int totalDownloaded = 0;
  final StringBuffer buffer = StringBuffer();

  final canUseAnsi = stdout.hasTerminal;

  if (canUseAnsi && !forceFinal && elapsed.inMilliseconds > 200) {
    for (int i = 0; i < numChunks + 2; i++) {
      buffer.write('\x1B[1A\x1B[2K');
    }
  }

  for (int i = 0; i < numChunks; i++) {
    final chunk = progressMap[i]!;
    totalDownloaded += chunk.downloadedBytes;
    final percent = (chunk.totalBytes == 0) ? 0.0 : (chunk.downloadedBytes / chunk.totalBytes).clamp(0.0, 1.0);
    final bar = _drawBar(percent, width: 20);

    final chunkMB = (chunk.downloadedBytes / (1024 * 1024)).toStringAsFixed(1);
    final totalMB = (chunk.totalBytes / (1024 * 1024)).toStringAsFixed(1);

    buffer.writeln('Chunk $i: [$bar] ${(percent * 100).toStringAsFixed(0)}% ($chunkMB/$totalMB MB)');
  }

  final overallPercent = (totalDownloaded / totalContentLength).clamp(0.0, 1.0);
  final overallBar = _drawBar(overallPercent, width: 30);
  final double elapsedSeconds = elapsed.inMilliseconds / 1000.0;
  final double speedMBps = elapsedSeconds > 0 ? (totalDownloaded / (1024 * 1024)) / elapsedSeconds : 0.0;

  final double remainingBytes = (totalContentLength - totalDownloaded).toDouble();
  final double remainingSeconds = speedMBps > 0 ? (remainingBytes / (1024 * 1024)) / speedMBps : 0;

  final totalMBFormatted = (totalContentLength / (1024 * 1024)).toStringAsFixed(1);
  final downloadedMBFormatted = (totalDownloaded / (1024 * 1024)).toStringAsFixed(1);

  buffer.writeln('Overall: [$overallBar] ${(overallPercent * 100).toStringAsFixed(1)}% ($downloadedMBFormatted/$totalMBFormatted MB)');
  buffer.writeln('Speed: ${speedMBps.toStringAsFixed(2)} MB/s | ETA: ${_formatDuration(remainingSeconds.round())}');

  stdout.write(buffer.toString());
}

String _drawBar(double percent, {int width = 20}) {
  final filledLength = (width * percent).round();
  final emptyLength = (width - filledLength).clamp(0, width);
  return '█' * filledLength + '░' * emptyLength;
}

String _formatDuration(int seconds) {
  if (seconds <= 0 || seconds.isInfinite || seconds.isNaN) return '0s';
  final duration = Duration(seconds: seconds);
  final mins = duration.inMinutes;
  final secs = duration.inSeconds % 60;
  return mins > 0 ? '${mins}m ${secs}s' : '${secs}s';
}