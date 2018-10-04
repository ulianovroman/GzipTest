using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;

namespace GZipTest
{
    
    public class FileDecompressor : FileProcessor
    {
        private volatile bool _finishedDecompress;
        public FileDecompressor(string inputPath, string outputPath, int maxBlocksinRam = int.MaxValue)
            : base(inputPath, outputPath, maxBlocksinRam)
        {
        }

        protected override void InnerExecute()
        {
            var threads = new Thread[Environment.ProcessorCount];

            for (int i = 0; i < threads.Length; ++i)
            {
                threads[i] = new Thread(DoWorkInParallel);

                threads[i].Start();
            }

            Thread writer = new Thread(WriteOutput)
            {
                Priority = ThreadPriority.Highest
            };

            writer.Start();
            using (var _inputStream = File.Open(_inputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                var sizeBuffer = new byte[4];
                int compressedBlockSize;

                for (int blockIndex = 0; _inputStream.Position < _inputStream.Length; ++blockIndex)
                {
                    if (Error != null)
                        return;

                    _inputStream.Read(sizeBuffer, 0, sizeBuffer.Length);
                    compressedBlockSize = BitConverter.ToInt32(sizeBuffer, 0);

                    if (compressedBlockSize <= 0)
                    {
                        throw new Exception("Perhaps the file is not a gzip archive or was compressed not by this program");
                    }

                    WaitReadIfNeed();

                    var content = new byte[compressedBlockSize];

                    _inputStream.Read(content, 0, compressedBlockSize);

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

            _finishedDecompress = true;

            writer.Join();
        }

        protected override byte[] ProcessBlock(byte[] block)
        {
            using (GZipStream decompressionStream = new GZipStream(new MemoryStream(block), CompressionMode.Decompress))
            {
                const int size = 4096;
                byte[] localBuffer = new byte[size];

                // На случай, если размер блока исходного файла нам неизвестен
                using (MemoryStream memory = new MemoryStream())
                {
                    int count = 0;
                    do
                    {
                        count = decompressionStream.Read(localBuffer, 0, size);
                        if (count > 0)
                        {
                            memory.Write(localBuffer, 0, count);
                        }
                    }
                    while (count > 0);

                    return memory.ToArray();
                }

            }
        }

        private void WriteOutput()
        {
            try
            {
                using (var _outputStream = File.Create(_outputFilePath))
                {
                    for (int i = 0; ; ++i, ++_currentWritingBlock)
                    {
                        int count = GetOutputBlocksCount();

                        if (count == 0 && _finishedDecompress)
                        {
                            break;
                        }

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

                        _outputStream.Write(block, 0, block.Length);
                    }
                }
            }
            catch (Exception e)
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
