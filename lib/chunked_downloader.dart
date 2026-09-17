import 'dart:async';
import 'dart:io';
import 'dart:isolate';
import 'dart:math' as math;
import 'package:http/http.dart' as http;
import 'package:path/path.dart' as p;

class ChunkProgress {
  int downloadedBytes = 0;
  int totalBytes = 0;
  bool isCompleted = false;
}

class DownloadProgressReport {
  final double overallProgress; // 0.0 to 1.0
  final int downloadedBytes;
  final int totalBytes;
  final double speedMBps;
  final Duration eta;
  final Map<int, ChunkProgress> chunkMap;

  DownloadProgressReport({
    required this.overallProgress,
    required this.downloadedBytes,
    required this.totalBytes,
    required this.speedMBps,
    required this.eta,
    required this.chunkMap,
  });
}

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

class Queue<T> {
  final List<T> _items = [];
  bool get isNotEmpty => _items.isNotEmpty;
  void add(T item) => _items.add(item);
  T removeFirst() => _items.removeAt(0);
}

class ChunkDownloader {
  final _progressController =
      StreamController<DownloadProgressReport>.broadcast();

  /// Exposes live progress updates
  Stream<DownloadProgressReport> get progressStream =>
      _progressController.stream;

  /// Starts downloading the file using multiple concurrent chunks.
  Future<File> download({
    required String url,
    required String savePath,
    int requestedChunks = 4,
    int maxConcurrency = 4,
    void Function(DownloadProgressReport)? onProgress,
  }) async {
    final client = http.Client();
    Timer? uiTimer;

    try {
      int contentLength = 0;
      bool serverSupportsRanges = false;

      try {
        final headResp = await client.head(
          Uri.parse(url),
          headers: {'User-Agent': 'Flutter Downloader Package'},
        );
        contentLength =
            int.tryParse(headResp.headers['content-length'] ?? '') ?? 0;
        serverSupportsRanges =
            (headResp.headers['accept-ranges']?.toLowerCase() == 'bytes');
      } catch (e) {
        throw Exception('HEAD request failed: $e');
      }

      if (contentLength == 0 || !serverSupportsRanges) {
        return await _singleStreamDownload(
          client,
          url,
          savePath,
          expectedLength: contentLength,
        );
      }

      final probeOk = await _probeRangeSupport(client, url);
      if (!probeOk) {
        return await _singleStreamDownload(
          client,
          url,
          savePath,
          expectedLength: contentLength,
        );
      }

      final numChunks = math.max(1, requestedChunks);
      final chunkSize = (contentLength / numChunks).ceil();
      final parentDir = File(savePath).parent.path;
      final tmpDir = Directory(
        p.join(parentDir, 'chunks_${p.basename(savePath)}'),
      );
      if (!tmpDir.existsSync()) tmpDir.createSync(recursive: true);

      final Map<int, ChunkProgress> progressMap = {};
      final Stopwatch stopwatch = Stopwatch()..start();

      for (int i = 0; i < numChunks; i++) {
        final start = i * chunkSize;
        final end = (i == numChunks - 1)
            ? contentLength - 1
            : (start + chunkSize - 1);
        progressMap[i] = ChunkProgress()..totalBytes = (end - start + 1);
      }

      uiTimer = Timer.periodic(const Duration(milliseconds: 200), (_) {
        _reportProgress(
          numChunks,
          progressMap,
          contentLength,
          stopwatch.elapsed,
          onProgress,
        );
      });

      final semaphore = Semaphore(math.max(1, maxConcurrency));
      final List<Future<void>> downloadTasks = [];
      for (int i = 0; i < numChunks; i++) {
        final start = i * chunkSize;
        final end = (i == numChunks - 1)
            ? contentLength - 1
            : (start + chunkSize - 1);
        final chunkPath = p.join(tmpDir.path, 'chunk_$i.part');

        downloadTasks.add(
          semaphore.withResource(
            () => _downloadChunk(
              client,
              url,
              start,
              end,
              chunkPath,
              progressMap[i]!,
            ),
          ),
        );
      }

      await Future.wait(downloadTasks);

      uiTimer.cancel();
      stopwatch.stop();
      _reportProgress(
        numChunks,
        progressMap,
        contentLength,
        stopwatch.elapsed,
        onProgress,
      );

      // Merge chunks inside a separate isolate to prevent UI stutters
      final outputFile = File(savePath);
      final tmpDirPath = tmpDir.path;

      // await Isolate.run(() async {
      //   final sink = outputFile.openWrite();
      //   try {
      //     for (int i = 0; i < numChunks; i++) {
      //       final chunkFile = File(p.join(tmpDirPath, 'chunk_$i.part'));
      //       if (!chunkFile.existsSync()) {
      //         throw Exception('Missing chunk $i file.');
      //       }
      //       await sink.addStream(chunkFile.openRead());
      //     }
      //   } finally {
      //     await sink.close();
      //   }
      // });
      // await Isolate.run(() => _mergeChunks(tmpDirPath, savePath, numChunks));
      // 1. In ChunkDownloader.download():
      // Replace your current Isolate.run call with this:
      await _mergeChunksInIsolate(tmpDirPath, savePath, numChunks);
      final finalSize = await outputFile.length();
      if (finalSize != contentLength) {
        throw Exception(
          'Merged file is $finalSize bytes, expected $contentLength bytes.',
        );
      }

      tmpDir.deleteSync(recursive: true);
      return outputFile;
    } finally {
      uiTimer?.cancel();
      client.close();
    }
  }

  void dispose() {
    _progressController.close();
  }

  Future<bool> _probeRangeSupport(http.Client client, String url) async {
    try {
      final request = http.Request('GET', Uri.parse(url));
      request.headers['Range'] = 'bytes=0-0';
      request.headers['User-Agent'] = 'Mozilla/5.0';
      final response = await client.send(request);
      await response.stream.drain();
      return response.statusCode == 206;
    } catch (_) {
      return false;
    }
  }

  Future<File> _singleStreamDownload(
    http.Client client,
    String url,
    String outputPath, {
    int? expectedLength,
    int maxRetries = 5,
  }) async {
    final file = File(outputPath);
    int retries = 0;

    while (true) {
      final existingBytes = file.existsSync() ? await file.length() : 0;
      if (expectedLength != null &&
          expectedLength > 0 &&
          existingBytes >= expectedLength) {
        return file;
      }

      try {
        final request = http.Request('GET', Uri.parse(url));
        request.headers['User-Agent'] = 'Mozilla/5.0';
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

        await for (final chunk in response.stream) {
          sink.add(chunk);
        }

        await sink.flush();
        await sink.close();
        return file;
      } catch (e) {
        retries++;
        if (retries > maxRetries) rethrow;
        await Future.delayed(Duration(seconds: 2 * retries));
      }
    }
  }

  Future<void> _downloadChunk(
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
        request.headers['User-Agent'] = 'Mozilla/5.0';

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
          await response.stream.drain();
          throw Exception('Server ignored Range header on chunk.');
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

  void _reportProgress(
    int numChunks,
    Map<int, ChunkProgress> progressMap,
    int totalContentLength,
    Duration elapsed,
    void Function(DownloadProgressReport)? onProgress,
  ) {
    int totalDownloaded = 0;
    for (int i = 0; i < numChunks; i++) {
      totalDownloaded += progressMap[i]!.downloadedBytes;
    }

    final overallPercent = (totalContentLength == 0)
        ? 0.0
        : (totalDownloaded / totalContentLength).clamp(0.0, 1.0);
    final double elapsedSeconds = elapsed.inMilliseconds / 1000.0;
    final double speedMBps = elapsedSeconds > 0
        ? (totalDownloaded / (1024 * 1024)) / elapsedSeconds
        : 0.0;

    final double remainingBytes = (totalContentLength - totalDownloaded)
        .toDouble();
    final double remainingSeconds = speedMBps > 0
        ? (remainingBytes / (1024 * 1024)) / speedMBps
        : 0;

    final report = DownloadProgressReport(
      overallProgress: overallPercent,
      downloadedBytes: totalDownloaded,
      totalBytes: totalContentLength,
      speedMBps: speedMBps,
      eta: Duration(seconds: remainingSeconds.round()),
      chunkMap: Map.from(progressMap),
    );

    _progressController.add(report);
    if (onProgress != null) onProgress(report);
  }
}

class _MergeArgs {
  final String tmpDirPath;
  final String savePath;
  final int numChunks;

  const _MergeArgs(this.tmpDirPath, this.savePath, this.numChunks);
}

/// MUST NOT be async! Synchronous file I/O prevents Dart from creating
/// unsendable _AsyncCompleter objects inside the isolate boundary.
void _syncMergeChunks(_MergeArgs args) {
  final outputFile = File(args.savePath);
  final sink = outputFile.openSync(mode: FileMode.write);

  try {
    final buffer = List<int>.filled(64 * 1024, 0); // 64 KB buffer

    for (int i = 0; i < args.numChunks; i++) {
      final chunkFile = File(p.join(args.tmpDirPath, 'chunk_$i.part'));

      if (!chunkFile.existsSync()) {
        throw Exception('Missing chunk $i file.');
      }

      final chunkStream = chunkFile.openSync(mode: FileMode.read);
      try {
        int bytesRead;
        while ((bytesRead = chunkStream.readIntoSync(buffer)) > 0) {
          sink.writeFromSync(buffer, 0, bytesRead);
        }
      } finally {
        chunkStream.closeSync();
      }
    }
  } finally {
    sink.closeSync();
  }
}

Future<void> _mergeChunksInIsolate(
  String tmpDirPath,
  String savePath,
  int numChunks,
) {
  return Isolate.run(
    () => _syncMergeChunks(_MergeArgs(tmpDirPath, savePath, numChunks)),
  );
}
