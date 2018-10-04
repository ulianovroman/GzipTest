using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

namespace GZipTest
{
    /// <summary>
    /// Базовый класс для архиватора\разархиватора
    /// </summary>
    public abstract class FileProcessor
    {
        protected readonly Queue<KeyValuePair<int, byte[]>> _inputBlocks;
        protected readonly Dictionary<int, byte[]> _outputBlocks;

        protected readonly ReaderWriterLockSlim _inputLock;
        protected readonly ReaderWriterLockSlim _outputLock;

        protected readonly string _outputFilePath;
        protected readonly string _inputFilePath;

        protected volatile bool _finishedReading;
        protected volatile int _currentWritingBlock;

        protected readonly int _maxBlocksinRam;
        protected readonly PerformanceCounter _avaliableRAMCounter;

        protected volatile Exception Error;
        public FileProcessor(string inputPath, string outputPath, int maxBlocksinRam)
        {
            _currentWritingBlock = 0;
            _inputBlocks = new Queue<KeyValuePair<int, byte[]>>();
            _outputBlocks = new Dictionary<int, byte[]>();
            _inputLock = new ReaderWriterLockSlim();
            _outputLock = new ReaderWriterLockSlim();
            _outputFilePath = outputPath;
            _inputFilePath = inputPath;
            _maxBlocksinRam = maxBlocksinRam;
            _avaliableRAMCounter = new PerformanceCounter("Memory", "Available MBytes");
        }

        protected abstract byte[] ProcessBlock(byte[] block);

        protected abstract void InnerExecute();
        public void Execute()
        {
            try
            {
                InnerExecute();
            }
            catch (Exception e)
            {
                Error = e;
            }
            finally
            {
                if (Error != null)
                {
                    throw Error;
                }
            }
        }

        protected void DoWorkInParallel()
        {
            try
            {
                while (true)
                {
                    KeyValuePair<int, byte[]>? block = null;

                    _inputLock.CallInWriteLock(() =>
                    {
                        if (_inputBlocks.Count != 0)
                        {
                            block = _inputBlocks.Dequeue();
                        }
                    });

                    if (block.HasValue)
                    {
                        var processedBlock = ProcessBlock(block.Value.Value);

                        WaitWriteIfNeed(block.Value.Key);

                        _outputLock.CallInWriteLock(() =>
                        {
                            _outputBlocks.Add(block.Value.Key, processedBlock);
                        });
                    }

                    int count = GetInputBlocksCount();

                    if ((count == 0 && _finishedReading) || Error != null)
                    {
                        break;
                    }
                }
            }
            catch(Exception e)
            {
                Error = e;
            }
        }
        
        protected int GetInputBlocksCount()
        {
            int count = 0;

            _inputLock.CallInReadLock(() =>
            {
                count = _inputBlocks.Count;
            });

            return count;
        }

        protected int GetOutputBlocksCount()
        {
            int count = 0;

            _outputLock.CallInReadLock(() =>
            {
                count = _outputBlocks.Count;
            });

            return count;
        }

        protected void WaitReadIfNeed()
        {
            while (_avaliableRAMCounter.NextValue() < 50 || GetInputBlocksCount() >= _maxBlocksinRam)
            {
                Thread.Sleep(1);
            }
        }

        protected void WaitWriteIfNeed(int blockIndex)
        {
            while (_avaliableRAMCounter.NextValue() < 50  || GetOutputBlocksCount() >= _maxBlocksinRam)
            {
                // Если пишущий поток ожидает именно этот блок, не ждём
                if (blockIndex == _currentWritingBlock)
                    return;

                Thread.Sleep(1);
            }
        }
    }
}
