package io.github.spottedleaf.concurrentutil.lock;

public interface SeqLock {

    public void acquireWrite();

    public boolean tryAcquireWrite();

    public void releaseWrite();

    public int acquireRead();

    public int tryAcquireRead();

    public boolean checkRead(final int read);

    default public boolean canRead(final int read) {
        return (read & 1) == 0;
    }
}