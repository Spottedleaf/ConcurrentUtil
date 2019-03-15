package ca.spottedleaf.concurrentutil.lock;

import ca.spottedleaf.concurrentutil.ConcurrentUtil;

import java.lang.invoke.VarHandle;

/**
 * SeqLock implementation offering the bare minimum required by the {@link SeqLock} specification.
 * WeakSeqLocks cannot be used concurrently with multiple writer threads. As such, {@link #acquireWrite()} has
 * the same effect as calling {@link #tryAcquireWrite()}. Writes are not guaranteed to be published immediately, and
 * loads can be re-ordered across write lock handling.
 */
public class WeakSeqLock implements SeqLock {

    protected int lock;

    protected static final VarHandle LOCK_HANDLE = ConcurrentUtil.getVarHandle(WeakSeqLock.class, "lock", int.class);

    protected final int getLockPlain() {
        return (int)LOCK_HANDLE.get(this);
    }

    protected final int getLockOpaque() {
        return (int)LOCK_HANDLE.getOpaque(this);
    }

    protected final void setLockOpaque(final int value) {
        LOCK_HANDLE.setOpaque(this, value);
    }

    public WeakSeqLock() {
        VarHandle.storeStoreFence();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void acquireWrite() {
        final int lock = this.getLockPlain();
        this.setLockOpaque(lock + 1);
        VarHandle.storeStoreFence();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean tryAcquireWrite() {
        this.acquireWrite();
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void releaseWrite() {
        final int lock = this.getLockPlain();
        VarHandle.storeStoreFence();
        this.setLockOpaque(lock + 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void abortWrite() {
        final int lock = this.getLockPlain();
        VarHandle.storeStoreFence();
        this.setLockOpaque(lock ^ 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int acquireRead() {
        int failures = 0;
        int curr;

        for (curr = this.getLockOpaque(); !this.canRead(curr); curr = this.getLockOpaque()) {
            for (int i = 0; i < failures; ++i) {
                Thread.onSpinWait();
            }

            if (++failures > 5_000) { /* TODO determine a threshold */
                Thread.yield();
            }
            /* Better waiting is beyond the scope of this lock; if it is needed the lock is being misused */
        }

        VarHandle.loadLoadFence();
        return curr;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean tryReleaseRead(final int read) {
        VarHandle.loadLoadFence();
        return this.getLockOpaque() == read;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getSequentialCounter() {
        final int lock = this.getLockOpaque();
        VarHandle.loadLoadFence();
        return lock;
    }
}
