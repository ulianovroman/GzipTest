using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace GZipTest
{
    public static class Extensions
    {
        public static void CallInReadLock(this ReaderWriterLockSlim @lock, Action action)
        {
            @lock.EnterReadLock();

            try
            {
                action();
            }
            finally
            {
                @lock.ExitReadLock();
            }
        }

        public static void CallInWriteLock(this ReaderWriterLockSlim @lock, Action action)
        {
            @lock.EnterWriteLock();

            try
            {
                action();
            }
            finally
            {
                @lock.ExitWriteLock();
            }
        }
    }
}
