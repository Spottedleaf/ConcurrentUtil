package io.github.spottedleaf.concurrentutil.misc;

import io.github.spottedleaf.concurrentutil.ConcurrentUtil;
import io.github.spottedleaf.concurrentutil.util.IntegerUtil;

import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

public class SynchronizationPoint {

    private final Thread[] threads;
    private final long allWaitingThreads;

    /** represents the threads which are currently executing (that is, threads not waiting on finish) */
    private volatile long runningThreads;

    /** represents the threads which have requested to execute alone */
    private volatile long requireAloneExecution;

    /** Threads which are waiting on this synchronization point */
    private volatile long synchronizingThreads;

    private long aloneThreadRentryCount = 0;

    private static final VarHandle RUNNING_THREADs = ConcurrentUtil.getVarHandle(SynchronizationPoint.class, "runningThreads", long.class);
    private static final VarHandle REQUIRE_ALONE_EXECUTION = ConcurrentUtil.getVarHandle(SynchronizationPoint.class, "requireAloneExecution", long.class);
    private static final VarHandle SYNCHRONIZING_THREADS = ConcurrentUtil.getVarHandle(SynchronizationPoint.class, "synchronizingThreads", long.class);

    private long compareAndExchangeSynchronizingThreadsVolatile(final long expect, final long update) {
        return (long)SYNCHRONIZING_THREADS.compareAndExchange(this, expect, update);
    }

    private long getAndBitwiseOrSynchronizingThreadsVolatile(final long param) {
        return (long)SYNCHRONIZING_THREADS.getAndBitwiseOr(this, param);
    }

    private long getAndBitwiseOrRunningThreadsVolatile(final long param) {
        return (long)RUNNING_THREADs.getAndBitwiseOr(this, param);
    }

    private long getAndBitwiseXorRunningThreadsVolatile(final long param) {
        return (long)RUNNING_THREADs.getAndBitwiseXor(this, param);
    }

    private long getRunningThreadsOpaque() {
        return (long)RUNNING_THREADs.getOpaque(this);
    }

    private long getRunningThreadsAcquire() {
        return (long)RUNNING_THREADs.getAcquire(this);
    }

    private long getRunningThreadsVolatile() {
        return (long)RUNNING_THREADs.getVolatile(this);
    }

    private long getSynchronizingThreadsOpaque() {
        return (long)SYNCHRONIZING_THREADS.getOpaque(this);
    }

    private long getSynchronizingThreadsVolatile() {
        return (long)SYNCHRONIZING_THREADS.getVolatile(this);
    }

    private long getRequireAloneExecutionOpaque() {
        return (long)REQUIRE_ALONE_EXECUTION.getOpaque(this);
    }

    private long getRequireAloneExecutionVolatile() {
        return (long)REQUIRE_ALONE_EXECUTION.getVolatile(this);
    }

    private void setRequireAloneExecutionVolatile(final long value) {
        REQUIRE_ALONE_EXECUTION.setOpaque(this, value);
    }

    private long getAndBitwiseOrRequireAloneExecutionVolatile(final long param) {
        return (long)REQUIRE_ALONE_EXECUTION.getAndBitwiseOr(param);
    }

    /**
     * Constructs a synchronization point
     * @param threads The threads which will be interacting with this synchronization point.
     */
    public SynchronizationPoint(final Thread[] threads) {
        final int nthreads = threads.length;
        if (nthreads > 64 || nthreads == 0) {
            throw new IllegalArgumentException("total threads out of range (0, 64]: " + nthreads);
        }
        this.allWaitingThreads = -1L >>> (64 - nthreads); /* mask that represents all threads waiting */
        this.threads = threads;
    }

    private void unparkAll(final int exceptFor) {
        for (int i = 0; i < exceptFor; ++i) {
            LockSupport.unpark(this.threads[i]);
        }
        for (int i = exceptFor + 1; i < this.threads.length; ++i) {
            LockSupport.unpark(this.threads[i]);
        }
    }

    private void unparkAll(long bitset) {
        while (bitset != 0) {
            final int id = IntegerUtil.floorLog2(bitset);
            bitset ^= (1L << id);
            LockSupport.unpark(this.threads[id]);
        }
    }

    public void start(final int id) {
        final long bitfield = 1L << id;
        final long prev = this.getAndBitwiseOrRunningThreadsVolatile(bitfield);
        if ((prev | bitfield) == prev) {
            throw new IllegalStateException("start is not re-entrant");
        }
        if ((prev | bitfield) != this.allWaitingThreads) {
            do {
                LockSupport.park();
            } while (this.getRunningThreadsAcquire() != this.allWaitingThreads); /* Use acquire to synchronize */
        } else {
            this.unparkAll(id);
        }
    }

    public void end(final int id) {
        final long bitfield = 1L << id;
        /* Ensure we've started */
        if ((this.getRunningThreadsOpaque() & bitfield) == 0) {
            throw new IllegalStateException("not yet started");
        }

        /* Make sure we're not missing an endAloneExecution() call */

        long synchronizingThreads = this.getSynchronizingThreadsOpaque(); /* volatile not required to see if our bitfield is set */
        if ((synchronizingThreads & bitfield) != 0) {
            throw new IllegalStateException("cannot end while synchronizing (missing endAloneExecution call)");
        }

        /* Remove from running threads */
        final long prev = this.getAndBitwiseXorRunningThreadsVolatile(bitfield);

        if (prev == bitfield) {
            /* All other threads have ended, we're the last one out which means we have to wake up all the other threads */
            this.unparkAll(id);
            return;
        }

        final long otherThreads = prev ^ bitfield;
        if (otherThreads == (synchronizingThreads = this.getSynchronizingThreadsVolatile())) { // TODO re-read and volatile?
            final long aloneThreads = this.getRequireAloneExecutionVolatile(); // TODO volatile? // TODO
            /* In this case all other threads are waiting on this thread to synchronize */
            /* We need to wake them up before we park */
            if (synchronizingThreads == this.compareAndExchangeSynchronizingThreadsVolatile(synchronizingThreads, 0)) {
                /* While there is no harm in double unparking the waiting threads, the performance of doing so is likely awful compared to a cas */
                /* so we ensure we don't double unpark by casing */
                this.unparkAll(otherThreads);
            }
        }

        do {
            LockSupport.park();
        } while (this.getRunningThreadsAcquire() != 0); /* Use acquire to synchronize */
    }

    public void weakEnter(final int id) {
        if (this.getSynchronizingThreadsOpaque() != 0) {
            this.waitEnter(id);
        }
    }

    public void waitEnter(final int id) {
        final long bitfield = 1L << id;
        if (this.getSynchronizingThreadsOpaque() == this.getRunningThreadsOpaque()) {
            // we are the alone thread executing (which makes the opaque reads fine)
            // it could be plain but that would risk non-atomic reads
            return;
        }
        final long synchronizing = this.getAndBitwiseOrSynchronizingThreadsVolatile(bitfield);
        if ((synchronizing | bitfield) != this.getRunningThreadsVolatile()) {
            /* Other threads are running */

            do {
                LockSupport.park();
            } while (this.getSynchronizingThreadsVolatile() != 0);
            return;
        }

        /* We're potentially (see race condition below) the only thread currently running */
        final long aloneThreads = this.getRequireAloneExecutionOpaque();
        if (aloneThreads != 0) {
            /* We are the only thread executing here */
            /* Start exiting */
            final int unpark = IntegerUtil.floorLog2(aloneThreads);
            LockSupport.unpark(this.threads[unpark]);

            do {
                LockSupport.park();
            } while (this.getSynchronizingThreadsVolatile() != 0);
            return;
        }

        if (synchronizing != this.compareAndExchangeSynchronizingThreadsVolatile(synchronizing | bitfield, 0)) {
            /* A thread calling end() has beaten us (race condition) to waking up all other threads */
            return;
        }

        this.unparkAll(synchronizing);
    }

    public void waitEnterExecuteAlone(final int id) {
        if (this.aloneThreadRentryCount != 0) {
            /* we are the alone thread executing and we are re-entrant safe */
            ++this.aloneThreadRentryCount;
            return;
        }

        final long bitfield = 1L << id;

        final long x = this.getAndBitwiseOrRequireAloneExecutionVolatile(bitfield);

    }

    public void endAloneExecution(final int id) {
        if (--this.aloneThreadRentryCount != 0) {
            /* we are the alone thread executing and we are re-entrant safe */
            return;
        }

        final long bitfield = 1L << id;

        /* Only unpark all synchronizing threads if no other thread wants to execute alone */
        final long synchronizing = this.getSynchronizingThreadsOpaque();
        final long alone = this.getRequireAloneExecutionOpaque();

        if (alone == 0) {
            this.unparkAll(synchronizing ^ bitfield);
            return;
        }

        final int wake = IntegerUtil.floorLog2(alone);
        this.setRequireAloneExecutionVolatile(wake ^ alone); // TODO volatile required?

        LockSupport.unpark(this.threads[wake]);

        do {
            LockSupport.park();
        } while (this.getSynchronizingThreadsVolatile() != 0);
    }
}