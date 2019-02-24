package io.github.spottedleaf.concurrentutil.misc;

import io.github.spottedleaf.concurrentutil.ConcurrentUtil;

import java.lang.invoke.VarHandle;

public class VolatileSeqLock implements SeqLock {

    protected volatile int lock;

    protected static final VarHandle LOCK_HANDLE = ConcurrentUtil.getVarHandle(VolatileSeqLock.class, "lock", int.class);

    protected final int getLockPlain() {
        return (int)LOCK_HANDLE.get(this);
    }

    protected final int getLockVolatile() {
        return (int)LOCK_HANDLE.getVolatile(this);
    }

    protected final int compareAndExchangeLockVolatile(final int expect, final int update) {
        return (int)LOCK_HANDLE.compareAndExchange(this, expect, update);
    }

    protected final void setLockVolatile(final int param) {
        LOCK_HANDLE.setVolatile(this, param);
    }

    @Override
    public boolean checkRead(final int read) {
        return this.getLockVolatile() == read;
    }

    @Override
    public int acquireRead() {
        int failures = 0;
        int curr;

        for (curr = this.getLockVolatile(); !this.canRead(curr); curr = this.getLockVolatile()) {
            for (int i = 0; i < failures; ++i) {
                Thread.onSpinWait();
            }

            if (++failures > 5_000) { /* TODO determine a threshold */
                Thread.yield();
            }
            /* Better waiting is beyond the scope of this lock; if it is needed the lock is being misused */
        }

        return curr;
    }

    @Override
    public void acquireWrite() {
        int failures = 0;

        for (int curr = this.getLockVolatile();;) {
            for (int i = 0; i < failures; ++i) {
                Thread.onSpinWait();
            }

            if (this.canRead(curr) && (curr == (curr = this.compareAndExchangeLockVolatile(curr, curr | 1)))) {
                return;
            }

            if (++failures > 5_000) { /* TODO determine a threshold */
                Thread.yield();
            }
        }

    }

    @Override
    public boolean tryAcquireWrite() {
        final int lock = this.getLockVolatile();
        return this.canRead(lock) && lock == this.compareAndExchangeLockVolatile(lock, lock + 1);
    }

    @Override
    public int tryAcquireRead() {
        return this.getLockVolatile();
    }

    @Override
    public void releaseWrite() {
        this.setLockVolatile(this.getLockPlain() + 1);
    }
}
