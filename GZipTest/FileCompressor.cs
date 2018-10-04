using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace GZipTest
{
    public class FileCompressor : FileProcessor
    {       
        private int _blocksCount;
        private readonly int _blockSize;

        public FileCompressor(string inputPath, string outputPath, int maxBlocksinRam = int.MaxValue)
            :base(inputPath, outputPath, maxBlocksinRam)
        {
            _blockSize = 1024 * 1024;
        }

        protected override void InnerExecute()
        {
            // Запускаем потоки на обработку блоков
            var threads = new Thread[Environment.ProcessorCount];

            for (int i = 0; i < threads.Length; ++i)
            {
                threads[i] = new Thread(DoWorkInParallel);

                threads[i].Start();
            }

            // Запускаем пишущий поток
            Thread writer = new Thread(WriteOutput)
            {
                Priority = ThreadPriority.Highest
            };

            using (var _inputStream = File.Open(_inputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                _blocksCount = (int)(_inputStream.Length / ((long)_blockSize));

                if (_inputStream.Length % ((long)_blockSize) != 0)
                {
                    ++_blocksCount;
                }

                writer.Start();

                for (long i = 0, blockIndex = 0; i < _inputStream.Length; i += _blockSize, ++blockIndex)
                {
                    if (Error != null)
                        return;

                    var length = i + _blockSize < _inputStream.Length
                        ? _blockSize
                        : (int)(_inputStream.Length - i);

                    WaitReadIfNeed();

                    var content = new byte[length];
                    _inputStream.Read(content, 0, length);

                    var block = new KeyValuePair<int, byte[]>((int)blockIndex, content);

                    _inputLock.CallInWriteLock(() =>
                    {                      
                        _inputBlocks.Enqueue(block);
                    });
                }
            }

            _finishedReading = true;

            foreach (var thread in threads)
            {
                thread.Join();
            }

            writer.Join();
        }

        protected override byte[] ProcessBlock(byte[] block)
        {
            using (var compressedFileStream = new MemoryStream())
            {
                using (GZipStream compressionStream = new GZipStream(compressedFileStream, CompressionMode.Compress))
                {
                    compressionStream.Write(block, 0, block.Length);
                }

                return compressedFileStream.ToArray();
            }
        }

        private void WriteOutput()
        {
            try
            {
                using (var _outputStream = File.Create(_outputFilePath))
                {
                    for (int i = 0; i < _blocksCount; ++i, ++_currentWritingBlock)
                    {
                        byte[] block = null;
                        while (true)
                        {
                            if (Error != null)
                                return;

                            var contains = false;

                            _outputLock.CallInReadLock(() =>
                            {
                                contains = _outputBlocks.ContainsKey(i);
                            });

                            if (!contains)
                            {
                                Thread.Sleep(1);
                                continue;
                            }

                            _outputLock.CallInWriteLock(() =>
                            {
                                block = _outputBlocks[i];
                                _outputBlocks.Remove(i);
                            });

                            break;
                        }

                        var length = BitConverter.GetBytes(block.Length);

                        _outputStream.Write(length, 0, length.Length);
                        _outputStream.Write(block, 0, block.Length);
                    }
                }
            }
            catch(Exception e)
            {
                Error = e;
                return;
            }
            finally
            {
                if (Error != null && File.Exists(_outputFilePath))
                {
                    try
                    {
                        File.Delete(_outputFilePath);
                    }
                    catch
                    {
                    }
                }
            }
        }

    }
}
