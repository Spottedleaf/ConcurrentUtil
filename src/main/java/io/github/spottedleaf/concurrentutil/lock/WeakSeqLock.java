package io.github.spottedleaf.concurrentutil.lock;

import io.github.spottedleaf.concurrentutil.ConcurrentUtil;

import java.lang.invoke.VarHandle;

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

    @Override
    public void acquireWrite() {
        final int lock = this.getLockPlain();
        this.setLockOpaque(lock + 1);
        VarHandle.storeStoreFence();
    }

    @Override
    public boolean tryAcquireWrite() {
        this.acquireWrite();
        return true;
    }

    @Override
    public void releaseWrite() {
        final int lock = this.getLockPlain();
        VarHandle.storeStoreFence();
        this.setLockOpaque(lock + 1);
    }

    @Override
    public int tryAcquireRead() {
        final int lock = this.getLockOpaque();

        VarHandle.loadLoadFence();

        return lock;
    }

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

    @Override
    public boolean checkRead(final int read) {
        VarHandle.loadLoadFence();

        return this.getLockOpaque() == read;
    }
}
