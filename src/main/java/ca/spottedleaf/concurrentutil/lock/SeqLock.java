package ca.spottedleaf.concurrentutil.lock;

import java.lang.invoke.VarHandle;

/**
 * SeqLocks are used to provide fast locking mechanism to multiple shared variables. A sequential counter
 * (sometimes returned directly by the methods defined in this interface) is used to control access to shared variables.
 *
 * <p>
 * The sequential counter is always initially 0, and it is always odd if a writer holds the lock.
 * In this case, other writers cannot obtain the lock until it is released and readers cannot continue. Readers,
 * when acquiring the read lock, will only read the sequential counter and ensure it is even before continuing.
 * Once reads are complete, the reader will re-check the sequential counter. If the counter has changed since it was
 * initially read, then the read is considered invalid and the reader must re-acquire the read lock and read again.
 * </p>
 *
 * <p>
 * Writers are required to acquire the "write lock" before issuing writes. Writers waiting for the write lock will wait
 * until the lock is released. Writers will only make writes to the shared variable data while the write lock is held.
 * Writers have the option to abort a write, but this can only be used if no writes have occurred to the shared variables.
 * When a write is aborted, the sequential counter is decremented.
 * </p>
 *
 * <p>
 * When SeqLocks are created, they will use a {@link VarHandle#storeStoreFence() STORE-STORE} fence to prevent re-ordering
 * of writes to shared data that occur before a SeqLock is created.
 * </p>
 *
 * <p>
 * Note that some implementations are not safe for use of multiple writers. See {@link WeakSeqLock}.
 * </p>
 *
 * <p>
 * SeqLocks do not require immediate publishing of writes, only consistency to readers. Implementations, such as
 * {@link VolatileSeqLock}, are free to make such visibility guarantees.
 * </p>
 *
 * <p>
 * Below is an example of a {@link VolatileSeqLock} used to control access to three variables, r1, r2, and r3.
 * </p>
 *
 * <p>
 * SeqLocks are not re-entrant, and re-entrant calls are undefined (such as acquiring a write/read lock twice).
 * Implementations can offer re-entrant guarantees.
 * </p>
 *
 * <p>
 * Minimum synchronization properties are described by the methods defined by this interface.
 * </p>
 *
 * <pre>
 *     class SeqLockUsage {
 *
 *         int r1, r2, r3;
 *
 *         final VolatileSeqLock seqlock;
 *
 *         SeqLockUsage() {
 *             r1 = 2; // example default value
 *             r2 = 5; // example default value
 *             r3 = 6; // example default value
 *             // required to be after shared data initialization to prevent re-ordering of writes
 *             // note that this does not have to be case if instances of this class are published
 *             // on final fields or through other synchronization guaranteeing correct publishing
 *             seqlock = new VolatileSeqLock();
 *         }
 *
 *         // reads and computes a value on r1, r2, and r3
 *         int computeValue() {
 *             int r1, r2, r3;
 *             int lock;
 *
 *             do {
 *                 lock = this.seqlock.acquireRead();
 *                 r1 = this.r1;
 *                 r2 = this.r2;
 *                 r3 = this.r3;
 *             } while (!this.seqlock.tryReleaseRead(lock));
 *
 *             return r1 * r2 * r3;
 *         }
 *
 *         void setValues(final int r1, final int r2, final int r3) {
 *             this.seqlock.acquireWrite();
 *             this.r1 = r1;
 *             this.r2 = r2;
 *             this.r3 = r3;
 *             // try-finally is good practice to use if exceptions can occur during writing.
 *             // In this case it is not possible, so it is not used.
 *             this.seqlock.releaseWrite();
 *         }
 *     }
 * </pre>
 * @see VolatileSeqLock
 * @see WeakSeqLock
 */
public interface SeqLock {

    /**
     * This function has undefined behaviour if the current thread owns the write lock.
     * This function will also have undefined behaviour if the current implementation does not allow multiple
     * threads to attempt to write and there are multiple threads attempting to acquire this SeqLock.
     * <p>
     * Eventually acquires the write lock. It is guaranteed that the write to the sequential counter
     * will be made with opaque or higher access. The write is also guaranteed to be followed by a {@link VarHandle#storeStoreFence() STORE-STORE}
     * fence, although it can use a stronger fence, or volatile access for the write.
     * </p>
     */
    public void acquireWrite();

    /**
     * This function has undefined behaviour if the current thread owns the write lock.
     * This function will also have undefined behaviour if the current implementation does not allow multiple
     * threads to attempt to write and there are multiple threads attempting to acquire this SeqLock.
     * <p>
     * Attempts to acquire the read lock. It is guaranteed that the write to the sequential counter, if any,
     * will be made with opaque or higher access. The write, if any, is also guaranteed to be followed by a {@link VarHandle#storeStoreFence() STORE-STORE}
     * fence, although it can use a stronger fence, or volatile access for the write.
     * </p>
     * <p>
     * There is no guaranteed synchronization to occur if the acquire of the SeqLock fails.
     * </p>
     * @return {@code true} if the seqlock was acquired, {@code false} otherwise.
     */
    public boolean tryAcquireWrite();

    /**
     * This function has undefined behaviour if the current thread does not own the write lock.
     * <p>
     * Increments the sequential counter indicating a write has completed. It is guaranteed that the write to the sequential counter
     * is made with opaque or higher access. The write is also guaranteed to be preceded by a {@link VarHandle#storeStoreFence() STORE-STORE}
     * fence, although it can use a stronger fence, or volatile access for the write.
     * </p>
     */
    public void releaseWrite();

    /**
     * This function has undefined behaviour if the current thread does not own the write lock.
     * <p>
     * Decrements the sequential counter indicating a write has not occurred. It is guaranteed that the write to the sequential counter
     * is made with opaque or higher access. The write is also guaranteed to be preceded by a {@link VarHandle#storeStoreFence() STORE-STORE}
     * fence, although it can use a stronger fence, or volatile access for the write.
     * </p>
     */
    public void abortWrite();

    /**
     * This function has undefined behaviour if the current thread already owns a read lock.
     * <p>
     * Eventually acquires the read lock and returns an even sequential counter. This function will spinwait until an
     * even sequential counter is read. The counter is required to be read with opaque or higher access. This function
     * is also guaranteed to use at least a {@link VarHandle#loadLoadFence() LOAD-LOAD} fence after reading the even counter,
     * although it can use a stronger fence, or volatile access for the read.
     * </p>
     * @return An even sequential counter.
     */
    public int acquireRead();

    /**
     * This function has undefined behaviour if the current thread does own a read lock.
     * <p>
     * Checks if the current sequential counter is equal to the specified counter. It is required that the counter is
     * read with opaque or higher access. This function is guaranteed to use at least a {@link VarHandle#loadLoadFence() LOAD-LOAD}
     * fence before reading the current counter, although it can use a stronger fence, or volatile access for the read.
     * </p>
     * @param read The specified counter.
     * @return {@code true} if the current sequential counter is equal to the specified counter, {@code false} otherwise.
     */
    public boolean tryReleaseRead(final int read);

    /**
     * Returns the current sequential counter. It is required that the counter is read with opaque or higher access.
     * This function is guaranteed to use at least a {@link VarHandle#loadLoadFence() LOAD-LOAD} fence after reading the
     * current counter, although it can use a stronger fence, or volatile access for the read.
     * @return The current sequential counter.
     */
    public int getSequentialCounter();

    /**
     * Checks if the sequential counter is even, which means readers may acquire the read lock.
     * @param read The sequential counter.
     * @return {@code true} if the counter is even, {@code false} if the counter is odd.
     */
    default public boolean canRead(final int read) {
        return (read & 1) == 0;
    }
}