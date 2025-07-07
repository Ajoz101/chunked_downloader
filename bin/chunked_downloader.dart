import 'dart:io';

import 'package:http/http.dart' as http;
import 'package:path/path.dart' as p;

void main(List<String> arg) async {
  if (arg.length < 3) {
    print("Not a link");
    return;
  }
  final url = arg[0];
  final outputPath = arg[1];
  final numChunks = int.parse(arg[2]) ?? 4;

  final client = http.Client();
  // Step 1: Get content length
  final headResp = await client.head(Uri.parse(url));
  final contentLength = int.tryParse(headResp.headers['content-length'] ?? '0') ?? 0;

  if (contentLength == 0) {
    print('Failed to get content length.');
    return;
  }

  print('downloading $contentLength bytes in $numChunks chunks...');

  final chunkSize = (contentLength / numChunks).ceil();
  final tmpDir = Directory('chunks_${p.basename(outputPath)}');

  if (!tmpDir.existsSync()) tmpDir.createSync();

  final List<Future<void>> downloadTasks = [];

  for (int i = 0; i < numChunks; i++) {
    final start = i * chunkSize;
    final end = (i + 1) * chunkSize - 1;
    final actualEnd = end >= contentLength ? contentLength - 1 : end;

    final chunkPath = p.join(tmpDir.path, 'chunk_$i.part');

    downloadTasks.add(
      downloadChunkWithResume(
        url,
        start,
        actualEnd,
        chunkPath,
        i,
      ),
    );
  }
  await Future.wait(downloadTasks);

  // Merge chunks
  final outputFile = File("D://aj_chunker/$outputPath");
  final sink = outputFile.openWrite();

  for (int i = 0; i < numChunks; i++) {
    final chunkFile = File(p.join(tmpDir.path, 'chunk_$i.part'));
    if (!chunkFile.existsSync()) {
      print('Missing chunk $i. Download failed.');
      return;
    }
    sink.add(await chunkFile.readAsBytes());
  }

  await sink.close();
  print('✅ Download completed: $outputPath');

  // Optionally delete temp chunks
  tmpDir.deleteSync(recursive: true);
}

Future<void> downloadChunkWithResume(
  String url,
  int start,
  int end,
  String filePath,
  int index, {
  int maxRetries = 5,
}) async {
  final file = File(filePath);

  // Skip if already downloaded
  if (file.existsSync() && file.lengthSync() == (end - start + 1)) {
    print('Chunk $index already downloaded, skipping...');
    return;
  }

  int retries = 0;
  while (retries <= maxRetries) {
    try {
      final client = http.Client();
      final headers = {
        'Range': 'bytes=$start-$end',
      };
        // print('Chunk $index downloading bytes ${(start / (1024 * 1024)).toStringAsFixed(2)}MB - ${(end / (1024 * 1024)).toStringAsFixed(2)}MB');


      print('Chunk $index downloadingbytes ${(start / (1024 * 1024)).toStringAsFixed(2)}MB - ${(end / (1024 * 1024)).toStringAsFixed(2)}MB (attempt ${retries + 1})');

      final response = await client.get(Uri.parse(url), headers: headers);

      if (response.statusCode == 206) {
        await file.writeAsBytes(response.bodyBytes);
        print('✅ Chunk $index downloaded.');
        return;
      } else {
        throw HttpException('Unexpected status: ${response.statusCode}');
      }
    } catch (e) {
      print('❌ Chunk $index failed: $e');
      retries++;
      if (retries <= maxRetries) {
        final delay = Duration(seconds: 2 * retries); // exponential backoff
        print('⏳ Retrying in ${delay.inSeconds}s...');
        await Future.delayed(delay);
      } else {
        print('❗ Chunk $index failed after $maxRetries retries.');
        rethrow;
      }
    }
  }
}