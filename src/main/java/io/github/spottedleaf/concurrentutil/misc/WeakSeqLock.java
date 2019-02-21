package io.github.spottedleaf.concurrentutil.misc;

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

    protected final void incrementLockCount() {
        final int lock = this.getLockPlain();
        VarHandle.storeStoreFence();
        this.setLockOpaque(lock + 1);
    }

    @Override
    public void acquireWrite() {
        this.incrementLockCount();
    }

    @Override
    public boolean tryAcquireWrite() {
        this.incrementLockCount();
        return true;
    }

    @Override
    public void releaseWrite() {
        this.incrementLockCount();
    }

    @Override
    public int tryAcquireRead() {
        VarHandle.loadLoadFence();
        return this.getLockOpaque();
    }

    @Override
    public int acquireRead() {
        VarHandle.loadLoadFence();

        int failures = 0;
        int curr;

        for (curr = this.getLockOpaque(); !this.canRead(curr); curr = this.getLockOpaque()) {
            for (int i = 0; i < failures; ++i) {
                Thread.onSpinWait();
            }

            //VarHandle.loadLoadFence();

            if (++failures > 5_000) { /* TODO determine a threshold */
                Thread.yield();
            }
            /* Better waiting is beyond the scope of this lock; if it is needed the lock is being misused */
        }

        return curr;
    }

    @Override
    public boolean checkRead(final int read) {
        VarHandle.loadLoadFence();

        return this.getLockOpaque() == read;
    }
}
