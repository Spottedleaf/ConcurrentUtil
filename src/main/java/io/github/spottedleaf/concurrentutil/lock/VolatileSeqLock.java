package io.github.spottedleaf.concurrentutil.lock;

import io.github.spottedleaf.concurrentutil.ConcurrentUtil;

import java.lang.invoke.VarHandle;

/**
 * SeqLock implementation which guarantees volatile access to the lock counter. This implementation also allows for
 * multiple writer threads to attempt to acquire the lock.
 * @see SeqLock
 */
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

    public VolatileSeqLock() {
        VarHandle.storeStoreFence();
    }

    /**
     * This function has undefined behaviour if the current thread owns the write lock.
     * <p>
     * Eventually acquires the write lock. It is guaranteed that the write to the sequential counter
     * will volatile.
     * </p>
     * <p>
     * Multiple threads can attempt to acquire the write lock concurrently.
     * </p>
     */
    @Override
    public void acquireWrite() {
        int failures = 0;

        for (int curr = this.getLockVolatile();;) {
            for (int i = 0; i < failures; ++i) {
                Thread.onSpinWait();
            }

            if (!this.canRead(curr)) {
                if (++failures > 5_000) { /* TODO determine a threshold */
                    Thread.yield();
                }
                curr = this.getLockVolatile();
                continue;
            }

            if ((curr == (curr = this.compareAndExchangeLockVolatile(curr, curr | 1)))) {
                return;
            }

            if (++failures > 5_000) { /* TODO determine a threshold */
                Thread.yield();
            }
        }
    }

    /**
     * This function has undefined behaviour if the current thread owns the write lock.
     * <p>
     * Attempts to acquire the read lock. It is guaranteed that the write to the sequential counter, if any,
     * will volatile. The read to the current sequential counter will be volatile.
     * </p>
     * <p>
     * Multiple threads can attempt to acquire the write lock concurrently.
     * </p>
     * @return {@inheritDoc}
     */
    @Override
    public boolean tryAcquireWrite() {
        final int lock = this.getLockVolatile();
        return this.canRead(lock) && lock == this.compareAndExchangeLockVolatile(lock, lock + 1);
    }

    /**
     * This function has undefined behaviour if the current thread does not own the write lock.
     * <p>
     * Increments the sequential counter indicating a write has completed. It is guaranteed that the write to the sequential counter
     * is volatile.
     * </p>
     */
    @Override
    public void releaseWrite() {
        this.setLockVolatile(this.getLockPlain() + 1);
    }

    /**
     * This function has undefined behaviour if the current thread does not own the write lock.
     * <p>
     * Decrements the sequential counter indicating a write has not occurred. It is guaranteed that the write to the sequential counter
     * is volatile.
     * </p>
     */
    @Override
    public void abortWrite() {
        this.setLockVolatile(this.getLockPlain() ^ 1);
    }

    /**
     * This function has undefined behaviour if the current thread already owns a read lock.
     * <p>
     * Eventually acquires the read lock and returns an even sequential counter. This function will spinwait until an
     * even sequential counter is read. The counter is read with volatile access.
     * </p>
     * @return {@inheritDoc}
     */
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

    /**
     * This function has undefined behaviour if the current thread does own a read lock.
     * <p>
     * Checks if the current sequential counter is equal to the specified counter. The counter is
     * read with volatile access.
     * </p>
     * @param read {@inheritDoc}
     * @return {@inheritDoc}
     */
    @Override
    public boolean tryReleaseRead(final int read) {
        return this.getLockVolatile() == read;
    }

    /**
     * Returns the sequential counter accessed with volatile semantics.
     */
    @Override
    public int getSequentialCounter() {
        return this.getLockVolatile();
    }
}
