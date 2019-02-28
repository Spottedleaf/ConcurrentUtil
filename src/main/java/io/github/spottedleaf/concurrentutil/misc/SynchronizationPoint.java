package io.github.spottedleaf.concurrentutil.misc;

import io.github.spottedleaf.concurrentutil.ConcurrentUtil;
import io.github.spottedleaf.concurrentutil.util.IntegerUtil;

import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

public class SynchronizationPoint {

    protected static final long WAKING_THREADS_BITFIELD = Long.MIN_VALUE;

    protected final Thread[] threads;
    protected final long allWaitingThreads;

    //@jdk.internal.vm.annotation.Contended
    protected volatile long runningThreads;

    //@jdk.internal.vm.annotation.Contended
    protected volatile long synchronizingThreads;

    //@jdk.internal.vm.annotation.Contended
    protected volatile long aloneExecutionThreads;

    protected volatile int synchronizingLockCount;
    protected volatile int startLockCount;

    protected int reEntryCount;

    protected static final VarHandle RUNNING_THREADS_HANDLE =
            ConcurrentUtil.getVarHandle(SynchronizationPoint.class, "runningThreads", long.class);
    protected static final VarHandle SYNCHRONIZING_THREADS_HANDLE =
            ConcurrentUtil.getVarHandle(SynchronizationPoint.class, "synchronizingThreads", long.class);
    protected static final VarHandle ALONE_EXECUTION_THREADS_HANDLE =
            ConcurrentUtil.getVarHandle(SynchronizationPoint.class, "aloneExecutionThreads", long.class);

    protected static final VarHandle SYNCHRONIZING_LOCK_COUNT_HANDLE =
            ConcurrentUtil.getVarHandle(SynchronizationPoint.class, "synchronizingLockCount", int.class);
    protected static final VarHandle START_LOCK_COUNT_HANDLE =
            ConcurrentUtil.getVarHandle(SynchronizationPoint.class, "startLockCount", int.class);

    /* running threads */

    protected final long getRunningThreadsOpaque() {
        return (long)RUNNING_THREADS_HANDLE.getOpaque(this);
    }

    protected final long getRunningThreadsVolatile() {
        return (long)RUNNING_THREADS_HANDLE.getVolatile(this);
    }

    protected final long getAndOrRunningThreadsVolatile(final long param) {
        return (long)RUNNING_THREADS_HANDLE.getAndBitwiseXor(this, param);
    }

    protected final long getAndXorRunningThreadsVolatile(final long param) {
        return (long)RUNNING_THREADS_HANDLE.getAndBitwiseXor(this, param);
    }

    /* synchronizing threads */

    protected final long getSynchronizingThreadsPlain() {
        return (long)SYNCHRONIZING_THREADS_HANDLE.get(this);
    }

    protected final long getSynchronizingThreadsOpaque() {
        return (long)SYNCHRONIZING_THREADS_HANDLE.getOpaque(this);
    }

    protected final long getSynchronizingThreadsVolatile() {
        return (long)SYNCHRONIZING_THREADS_HANDLE.getVolatile(this);
    }

    protected final long compareAndExchangeSynchronizingThreadsVolatile(final long expect, final long update) {
        return (long)SYNCHRONIZING_THREADS_HANDLE.compareAndExchange(this, expect, update);
    }

    protected final long getAndOrSynchronizingThreadsVolatile(final long param) {
        return (long)SYNCHRONIZING_THREADS_HANDLE.getAndBitwiseOr(this, param);
    }

    /* alone execution threads */

    protected final long getAloneExecutionThreadsPlain() {
        return (long)ALONE_EXECUTION_THREADS_HANDLE.get(this);
    }

    protected final long getAloneExecutionThreadsVolatile() {
        return (long)ALONE_EXECUTION_THREADS_HANDLE.getVolatile(this);
    }

    protected final void setAloneExecutionThreadsPlain(final long value) {
        ALONE_EXECUTION_THREADS_HANDLE.set(this, value);
    }

    protected final void setAloneExecutionThreadsVolatile(final long value) {
        ALONE_EXECUTION_THREADS_HANDLE.setVolatile(this, value);
    }

    protected final long getAndOrAloneExecutionThreadsVolatile(final long param) {
        return (long)ALONE_EXECUTION_THREADS_HANDLE.getAndBitwiseOr(this, param);
    }

    /* synchronizing lock count */

    protected final int getSynchronizingLockCountPlain() {
        return (int)SYNCHRONIZING_LOCK_COUNT_HANDLE.get(this);
    }

    protected final int getSynchronizingLockCountVolatile() {
        return (int)SYNCHRONIZING_LOCK_COUNT_HANDLE.getVolatile(this);
    }

    protected final void setSynchronizingLockCountVolatile(final int value) {
        SYNCHRONIZING_LOCK_COUNT_HANDLE.setVolatile(this, value);
    }

    protected final int compareAndExchangeSynchronizingLockCountVolatile(final int expect, final int update) {
        return (int)SYNCHRONIZING_LOCK_COUNT_HANDLE.compareAndExchange(this, expect, update);
    }

    /* start lock count */

    protected final int getStartLockCountPlain() {
        return (int)START_LOCK_COUNT_HANDLE.get(this);
    }

    protected final int getStartLockCountVolatile() {
        return (int)START_LOCK_COUNT_HANDLE.getVolatile(this);
    }

    protected final void setStartLockCountVolatile(final int value) {
        START_LOCK_COUNT_HANDLE.setVolatile(this, value);
    }

    /**
     * Constructs a synchronization point
     * @param threads The threads which will be interacting with this synchronization point.
     */
    public SynchronizationPoint(final Thread[] threads) {
        final int nthreads = threads.length;
        if (nthreads >= 64 || nthreads == 0) {
            throw new IllegalArgumentException("total threads out of range (0, 64): " + nthreads);
        }
        this.allWaitingThreads = -1L >>> (64 - nthreads); /* mask that represents all threads waiting */
        this.threads = threads;
    }

    protected final void unparkAll(final int exceptFor) {
        for (int i = 0; i < exceptFor; ++i) {
            LockSupport.unpark(this.threads[i]);
        }
        for (int i = exceptFor + 1, len = this.threads.length; i < len; ++i) {
            LockSupport.unpark(this.threads[i]);
        }
    }

    protected final void unparkAll(long bitset) {
        for (int i = 0, times = Long.bitCount(bitset); i < times; ++i) {
            final int leading = Long.numberOfLeadingZeros(bitset);
            bitset ^= (IntegerUtil.HIGH_BIT_U64 >>> leading); // inlined IntegerUtil#roundFloorLog2(long)
            LockSupport.unpark(this.threads[63 ^ leading]); // inlined IntegerUtil#floorLog2(long)
        }
    }

    /** @return {@code true} if the calling thread can continue, {@code false} otherwise */
    protected final boolean wakeThreads(final int id) {
        final long aloneThreads = this.getAloneExecutionThreadsPlain();

        if (aloneThreads == 0) {
            /* no more alone threads to execute, we can continue */

            this.setSynchronizingLockCountVolatile(this.getSynchronizingLockCountPlain() + 1);
            /* remove our bitfield and the field for indicating we're waking the threads */
            /* We use AND for our bitfield since it's not guaranteed that ours is in the synchronizing threads (see end()) */
            this.unparkAll((this.getSynchronizingThreadsPlain() ^ WAKING_THREADS_BITFIELD) & ~(1L << id));

            return true;
        }

        final int leading = Long.numberOfLeadingZeros(aloneThreads);

        /* we need to set this before unpark() so the alone thread can wake up from park() */
        this.setAloneExecutionThreadsVolatile(aloneThreads ^ (IntegerUtil.HIGH_BIT_U64 >>> leading)); // inlined IntegerUtil#roundFloorLog2(long)
        LockSupport.unpark(this.threads[63 ^ leading]); // inlined IntegerUtil#floorLog2(long)

        /* indicate there are alone threads executing (possibly) */

        return false;
    }

    public void start(final int id) {
        final long bitfield = 1L << id;
        final int lockCount = this.getStartLockCountPlain();

        final long runningThreads = this.getAndOrRunningThreadsVolatile(bitfield) | bitfield;

        if (runningThreads != this.allWaitingThreads) {
            do {
                LockSupport.park();
            } while (this.getStartLockCountVolatile() == lockCount);
        } else {
            this.setStartLockCountVolatile(lockCount + 1);
            this.unparkAll(id);
        }
    }

    public void end(final int id) {
        final long bitfield = 1L << id;
        final int lockCount = this.getStartLockCountPlain();

        final long runningThreads = this.getAndXorRunningThreadsVolatile(bitfield) ^ bitfield;

        if (runningThreads == 0) {
            /* Last thread running */
            this.setStartLockCountVolatile(lockCount + 1);
            this.unparkAll(id);

            return;
        }

        final long synchronizingThreads = this.getSynchronizingThreadsOpaque();

        if (synchronizingThreads == runningThreads
                && synchronizingThreads == this.compareAndExchangeSynchronizingThreadsVolatile(synchronizingThreads, synchronizingThreads | WAKING_THREADS_BITFIELD)) {
            this.wakeThreads(id);
        }

        do {
            LockSupport.park();
        } while (this.getStartLockCountVolatile() == lockCount);
    }

    public void weakEnter(final int id) {
        /* Use volatile to prevent re-ordering of this call */
        if (this.getSynchronizingThreadsVolatile() != 0) {
            this.enter(id);
        }
    }

    public void enter(final int id) {
        final long bitfield = 1L << id;
        final int lockCount = this.getSynchronizingLockCountPlain();

        if (this.reEntryCount != 0) {
            // we are the alone thread executing, we are re-entrant
            return;
        }

        final long synchronizing = this.getAndOrSynchronizingThreadsVolatile(bitfield) | bitfield;
        if (synchronizing != this.getRunningThreadsOpaque()) {
            /* Other threads are running */
            do {
                LockSupport.park();
            } while (this.getSynchronizingLockCountVolatile() == lockCount);
        }
        /* potentially the only thread executing */
        if (synchronizing != this.compareAndExchangeSynchronizingThreadsVolatile(synchronizing, synchronizing | WAKING_THREADS_BITFIELD)) {
            /* Lost cas, so we assume an alone thread could execute */
            do {
                LockSupport.park();
            } while (this.getSynchronizingLockCountVolatile() == lockCount);
        } else {
            /* Won cas, now we must wake up the other threads */
            if (this.wakeThreads(id)) {
                return;
            }

            do {
                LockSupport.park();
            } while (this.getSynchronizingLockCountVolatile() == lockCount);
        }
    }

    public void enterAlone(final int id) {
        final long bitfield = 1L << id;

        if (this.reEntryCount != 0) {
            /* We are the alone thread executing */
            ++this.reEntryCount;
            return;
        }

        /* add to the alone thread execution list */
        this.getAndOrAloneExecutionThreadsVolatile(bitfield);

        final long synchronizing = this.getAndOrSynchronizingThreadsVolatile(bitfield);

        if (synchronizing != this.getRunningThreadsVolatile()) {
            /* Other threads are running */
            do {
                LockSupport.park();
            } while ((this.getAloneExecutionThreadsVolatile() & bitfield) != 0);
            return;
        }

        /* potentially the only thread executing */
        if (synchronizing != this.compareAndExchangeSynchronizingThreadsVolatile(synchronizing, synchronizing | WAKING_THREADS_BITFIELD)) {
            /* Lost cas, this means another alone thread could execute */
            do {
                LockSupport.park();
            } while ((this.getAloneExecutionThreadsVolatile() & bitfield) != 0);
        } else {
            /* won cas, it's our job to wake up the threads */
            /* however that is postponed until this alone thread has finished its execution */
            this.reEntryCount = 1; /* set this for re-entry checks */
        }
    }

    public void endAloneExecution(final int id) {
        if (--this.reEntryCount != 0) {
            /* still re-entrant */
            return;
        }

        final int lockCount = this.getSynchronizingLockCountPlain();

        if (this.wakeThreads(id)) {
            /* synchronizing threads have been unparked */
            return;
        }

        do {
            LockSupport.park();
        } while (this.getStartLockCountVolatile() == lockCount);
    }
}