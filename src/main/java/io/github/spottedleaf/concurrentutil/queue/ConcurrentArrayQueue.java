package io.github.spottedleaf.concurrentutil.queue;

import io.github.spottedleaf.concurrentutil.ConcurrentUtil;
import io.github.spottedleaf.concurrentutil.util.IntegerUtil;

import java.lang.invoke.VarHandle;
import java.util.Arrays;

@Deprecated
/** @deprecated until further revised */
public final class ConcurrentArrayQueue<E> {

    public static final int NORESIZE_FLAG = (1 << 0);
    public static final int PEEK_FLAG     = (1 << 1);

    private static final int UINT_MAX = -1;
    private static final int MAX_INDEX = UINT_MAX >>> 2; /* 1 bit for encoded length, 1 bit for resize flag */
    private static final int MAX_LENGTH = MAX_INDEX + 1;
    private static final int INDEX_LENGTH_MASK = MAX_INDEX | MAX_LENGTH;

    private static final int READING_BIT = Integer.MIN_VALUE;

    private Object[] elements; /* access synchronized through indices */

    /* Separate the indices into different cache lines */
    private final int[] indices = new int[4 * (256 / 4)];


    /**
     * The first element in this queue.
     */
    private static final int AVAILABLE_HEAD_INDEX_INDEX = 1 * (256 / 4);

    /**
     * The ready-to-read last element added to this queue.
     */
    private static final int AVAILABLE_TAIL_INDEX_INDEX = 2 * (256 / 4);

    /**
     * The last element index allocated for writing.
     */
    private static final int ALLOCATED_TAIL_INDEX_INDEX = 3 * (256 / 4);

    private static final VarHandle INDICES_HANDLE = ConcurrentUtil.getArrayHandle(int[].class);

    public ConcurrentArrayQueue() {
        this(32);
    }

    public ConcurrentArrayQueue(int capacity) {
        if (capacity < 0 || capacity > MAX_LENGTH) {
            throw new IllegalArgumentException("Capacity " + capacity + " is out of bounds");
        } else if (capacity <= 32) {
            capacity = 32;
        } else {
            final int prevCapacity = capacity;
            capacity = IntegerUtil.roundCeilLog2(capacity);
            if (capacity > MAX_LENGTH) {
                throw new IllegalArgumentException("Capacity " + prevCapacity + " is out of bounds");
            }
        }

        this.elements = new Object[capacity];
        INDICES_HANDLE.set(this.indices, AVAILABLE_HEAD_INDEX_INDEX, capacity);
        INDICES_HANDLE.set(this.indices, AVAILABLE_TAIL_INDEX_INDEX, capacity);
        INDICES_HANDLE.set(this.indices, ALLOCATED_TAIL_INDEX_INDEX, capacity);
        /*HEAD_INDEX_HANDLE.set(this, capacity);
        AVAILABLE_TAIL_INDEX_HANDLE.set(this, capacity);
        ALLOCATED_TAIL_INDEX_HANDLE.set(this, capacity);*/
    }

    /* Inclusive head, Exclusive tail */
    private static int queueLength(final int head, final int tail, final int lengthMask) {
        return (tail - head) & lengthMask;
    }

    /* Inclusive head, Exclusive tail */
    private static int getRemainingLength(final int head, final int tail, final int length) {
        if (head <= tail) {
            return length - (tail - head);
        } else {
            return (head - tail);
        }
    }

    public int capacity() {
        int failures = 0;
        for (;;) {
            //final int tail = (int)ALLOCATED_TAIL_INDEX_HANDLE.getVolatile(this);
            final int tail = (int)INDICES_HANDLE.getVolatile(this.indices, ALLOCATED_TAIL_INDEX_INDEX);

            if (tail == UINT_MAX) {
                if (++failures > 500) {
                    failures = 0;
                    Thread.yield();
                }
                ConcurrentUtil.pause();
                continue;
            }

            return IntegerUtil.roundFloorLog2(tail);
        }
    }

    public int size() {
        int failures = 0;
        for (;;) {
            /*int head = (int)HEAD_INDEX_HANDLE.getVolatile(this);
            final int tail = (int)AVAILABLE_TAIL_INDEX_HANDLE.getAcquire(this);*/
            int head = (int)INDICES_HANDLE.getVolatile(this.indices, AVAILABLE_HEAD_INDEX_INDEX);
            final int tail = (int)INDICES_HANDLE.getAcquire(this.indices, AVAILABLE_TAIL_INDEX_INDEX);

            if (head == UINT_MAX) {
                ConcurrentUtil.pause();
                continue;
            }

            if (tail == UINT_MAX) {
                if (++failures > 500) {
                    failures = 0;
                    Thread.yield();
                }
                ConcurrentUtil.pause();
                continue;
            }

            head &= INDEX_LENGTH_MASK;

            final int lengthHead = IntegerUtil.roundFloorLog2(head);
            final int lengthTail = IntegerUtil.roundFloorLog2(tail);

            if (lengthHead != lengthTail) {
                ConcurrentUtil.pause();
                continue;
            }

            return queueLength(head ^ lengthHead, tail ^ lengthTail, lengthHead - 1);
        }
    }

    public int remainingCapacity() {
        int failures = 0;
        for (;;) {
            /*int head = (int)HEAD_INDEX_HANDLE.getVolatile(this);
            final int tail = (int)AVAILABLE_TAIL_INDEX_HANDLE.getAcquire(this);*/
            int head = (int)INDICES_HANDLE.getVolatile(this.indices, AVAILABLE_HEAD_INDEX_INDEX);
            final int tail = (int)INDICES_HANDLE.getAcquire(this.indices, AVAILABLE_TAIL_INDEX_INDEX);

            if (head == UINT_MAX) {
                ConcurrentUtil.pause();
                continue;
            }

            if (tail == UINT_MAX) {
                if (++failures > 500) {
                    failures = 0;
                    Thread.yield();
                }
                ConcurrentUtil.pause();
                continue;
            }

            head &= INDEX_LENGTH_MASK;

            final int lengthHead = IntegerUtil.roundFloorLog2(head);
            final int lengthTail = IntegerUtil.roundFloorLog2(tail);

            if (lengthHead != lengthTail) {
                ConcurrentUtil.pause();
                continue;
            }

            return getRemainingLength(head ^ lengthHead, tail ^ lengthTail, lengthHead);
        }
    }

    private int acquireHead(final int flags) {
        int head;
        if ((flags & NORESIZE_FLAG) != 0) {
            //head = (int)HEAD_INDEX_HANDLE.getVolatile(this);
            head = (int)INDICES_HANDLE.getVolatile(this.indices, AVAILABLE_HEAD_INDEX_INDEX);
        } else {
            for (int failures = 0;;++failures) {
                //head = (int)HEAD_INDEX_HANDLE.getAndBitwiseOr(this, READING_BIT);
                head = (int)INDICES_HANDLE.getAndBitwiseOr(this.indices, AVAILABLE_HEAD_INDEX_INDEX, READING_BIT);
                if ((head & READING_BIT) == 0) {
                    break;
                }
                for (int i = 0; i < failures; ++i) {
                    ConcurrentUtil.pause();
                }
            }
        }
        return head;
    }

    private void releaseHead(final int oldHead, final int newHead, final int length, final int flags) {
        if ((flags & PEEK_FLAG) != 0) {
            /* Only write when there is a need to */
            if ((flags & NORESIZE_FLAG) == 0) {
                //HEAD_INDEX_HANDLE.setVolatile(this, head | length);
                INDICES_HANDLE.setVolatile(this.indices, AVAILABLE_HEAD_INDEX_INDEX, oldHead | length);
            }
        } else {
            //HEAD_INDEX_HANDLE.setVolatile(this, newHead | length);
            INDICES_HANDLE.setVolatile(this.indices, AVAILABLE_HEAD_INDEX_INDEX, newHead | length);
        }
    }

    public int read(final E[] buffer, final int maxItems, final int flags) {
        int head = this.acquireHead(flags);

        /* Since writes to tail are seq_cst, and sychronization already occured, tail is pretty up-to-date */
        /* Use acquire to synchronize with the state at tail's point in time */
        //int tail = (int)AVAILABLE_TAIL_INDEX_HANDLE.getAcquire(this);
        int tail = (int)INDICES_HANDLE.getAcquire(this.indices, AVAILABLE_TAIL_INDEX_INDEX);

        /* By locking the head it is guaranteed the tail will have the same length as head */
        final Object[] elements = this.elements;

        final int length = IntegerUtil.roundFloorLog2(head);
        final int lengthMask = length - 1;

        tail ^= length;
        head ^= length;

        int itemsToRead = queueLength(head, tail, lengthMask);

        if (itemsToRead >= maxItems) {
            itemsToRead = maxItems;
        }

        final int newHead = (head + itemsToRead) & lengthMask;

        if (newHead >= head) {
            /* Sequential */
            if (buffer != null) {
                System.arraycopy(elements, head, (Object[]) buffer, 0, itemsToRead);
            }
            if ((flags & PEEK_FLAG) == 0) {
                Arrays.fill(elements, head, head + itemsToRead, null);
            }
        } else {
            /* Wrapped */
            final int end = length - head;
            if ((flags & PEEK_FLAG) != 0) {
                if (buffer != null) {
                    System.arraycopy(elements, head, (Object[]) buffer, 0, end);
                    System.arraycopy(elements, 0, (Object[]) buffer, end, itemsToRead - end);
                }
            } else {
                if (buffer != null) {
                    System.arraycopy(elements, head, (Object[]) buffer, 0, end);
                }
                Arrays.fill(elements, head, head + end, null);
                if (buffer != null) {
                    System.arraycopy(elements, 0, (Object[]) buffer, end, itemsToRead - end);
                }
                Arrays.fill(elements, 0, itemsToRead - end, null);
            }
        }

        this.releaseHead(head, newHead, length, flags);

        return itemsToRead;
    }

    @SafeVarargs
    public final int add(final E... elements) {
        return this.add(elements, 0, elements.length, 0);
    }

    public int add(final E[] elements, final int off, final int nitems, final int flags) {
        final int maxLength = MAX_LENGTH;

        if (nitems >= maxLength) {
            throw new IllegalArgumentException("nitems is too large: " + nitems);
        }

        int currentIndex = (int)INDICES_HANDLE.getVolatile(this.indices, ALLOCATED_TAIL_INDEX_INDEX);
        int failures = 0;
        for (;;) {
            /* Exponential backoff */
            for (int i = 0; i < failures; ++i) {
                ConcurrentUtil.pause();
            }
            /* Retrieve current_index after backoff to ensure it's not immediately out-of-date */
            /* Nevermind this drops perf by 4x */
            //currentIndex = (int)ALLOCATED_TAIL_INDEX_HANDLE.getVolatile(this);
            //currentIndex = (int)INDICES_HANDLE.getVolatile(this.indices, ALLOCATED_TAIL_INDEX_INDEX);

            if (currentIndex == UINT_MAX) {
                Thread.yield();
                //currentIndex = (int)ALLOCATED_TAIL_INDEX_HANDLE.getVolatile(this);
                currentIndex = (int)INDICES_HANDLE.getVolatile(this.indices, ALLOCATED_TAIL_INDEX_INDEX);
            }

            /* Not resizing */

            /* Decode length */
            final int length = IntegerUtil.roundFloorLog2(currentIndex);
            final int lengthMask = length - 1;

            final int allocatedStart = currentIndex ^ length; /* Inclusive */
            final int allocatedEnd = (currentIndex + nitems) & lengthMask; /* Exclusive */

            //final int currentHeadRaw = (int)HEAD_INDEX_HANDLE.getAcquire(this);
            final int currentHeadRaw = (int)INDICES_HANDLE.getAcquire(this.indices, AVAILABLE_HEAD_INDEX_INDEX);

            /* Validate the indices */

            /*
             * May not be resizing
             * Must have equal encoded lengths (otherwise size check is invalid)
             */

            if (currentHeadRaw == UINT_MAX) {
                ConcurrentUtil.pause();
                currentIndex = (int)INDICES_HANDLE.getVolatile(this.indices, ALLOCATED_TAIL_INDEX_INDEX);
                continue;
            }
            if (IntegerUtil.roundFloorLog2(currentHeadRaw & INDEX_LENGTH_MASK) != length) {
                ConcurrentUtil.pause();
                currentIndex = (int)INDICES_HANDLE.getVolatile(this.indices, ALLOCATED_TAIL_INDEX_INDEX);
                continue;
            }

            /* Finished validating the head index */

            final int queueStart = currentHeadRaw & lengthMask; /* Inclusive */

            /* Find remaining length to ensure there is enough space */
            final int remainingLength = getRemainingLength(queueStart, allocatedStart, length);

            if (remainingLength > nitems) {
                /* space available */
                final int prevCurrIndex = currentIndex;
                if ((currentIndex = (int)INDICES_HANDLE.compareAndExchange(this.indices, ALLOCATED_TAIL_INDEX_INDEX, currentIndex, allocatedEnd | length)) != prevCurrIndex) {
                    ++failures;
                    continue;
                }

                final Object[] queuedElements = this.elements;
                if (allocatedEnd >= allocatedStart) {
                    /* Sequential */
                    System.arraycopy((Object[])elements, off, queuedElements, allocatedStart, nitems);
                } else {
                    /* Wrapped */
                    final int end = length - allocatedStart;
                    System.arraycopy((Object[])elements, off, queuedElements, allocatedStart, end);
                    System.arraycopy((Object[])elements, off + end, queuedElements, 0, nitems - end);
                }

                while ((int)INDICES_HANDLE.getVolatile(this.indices, AVAILABLE_TAIL_INDEX_INDEX) != currentIndex) {
                    ConcurrentUtil.pause();
                }
                //AVAILABLE_TAIL_INDEX_HANDLE.setVolatile(this, allocatedEnd | length);
                INDICES_HANDLE.setVolatile(this.indices, AVAILABLE_TAIL_INDEX_INDEX, allocatedEnd | length);
                return length - remainingLength;
            }

            final int requiredLength = (length - remainingLength) + nitems; /* Exclusive end of queue once resized and copied */

            if ((flags & NORESIZE_FLAG) != 0 || requiredLength >= maxLength) {
                return -1; /* TODO */
            }

            /* Attempt to lock allocated_tail_index */
            //if (!ALLOCATED_TAIL_INDEX_HANDLE.compareAndSet(this, currentIndex, UINT_MAX)) {
            if (!INDICES_HANDLE.compareAndSet(this.indices, ALLOCATED_TAIL_INDEX_INDEX, currentIndex, UINT_MAX)) {
                ++failures;
                continue;
            }

            /* Add 1 to ensure that the resized queue will not be full */
            /* It is an error if this queue is full as distinction between a full queue and an empty one is not possible */
            /* exclusive end == inclusive start for both cases */
            final int expectedLength = IntegerUtil.roundCeilLog2(requiredLength + 1);
            final Object[] newElements = new Object[expectedLength];
            final Object[] currElements = this.elements;

            /* Copy previous elements over */
            /* Note that while new indices cannot be allocated, some writes to the queue may not be finished */
            /* wait for previous writes to complete */

            /* Use acquire to acquire with the writes to elements (thus synchronizing with other writers) */
            //while ((int)AVAILABLE_TAIL_INDEX_HANDLE.getAcquire(this) != currentIndex) {
            int spinWaitFailures = 0;
            while ((int)INDICES_HANDLE.getAcquire(this.indices, AVAILABLE_TAIL_INDEX_INDEX) != currentIndex) {
                if (spinWaitFailures == 1000) {
                    spinWaitFailures = 0;
                    Thread.yield();
                    /* This will not fix the thread scheduling issues however it should prevent total loss of performance */
                }
                ConcurrentUtil.pause();
                ++spinWaitFailures;
            }

            /* Copy the elements over */

            /* allocatedStart at this point is now the exclusive end of the queue */

            if (queueStart < allocatedStart) {
                /* Sequential */
                System.arraycopy(currElements, queueStart, newElements, 0, length - remainingLength);
            } else {
                /* Wrapped */
                final int end = length - queueStart;
                System.arraycopy(currElements, queueStart, newElements, 0, end);
                System.arraycopy(currElements, 0, newElements, end, allocatedStart);
            }

            /* Copy our elements over */

            System.arraycopy((Object[])elements, off, newElements, length - remainingLength, nitems);

            /* Update head */

            //for (int headcurr = (int)HEAD_INDEX_HANDLE.getVolatile(this);;) {
            for (int headcurr = (int)INDICES_HANDLE.getVolatile(this.indices, AVAILABLE_HEAD_INDEX_INDEX);;) {
                if ((headcurr & READING_BIT) != 0) {
                    Thread.yield();
                    headcurr = (int)INDICES_HANDLE.getAcquire(this.indices, AVAILABLE_HEAD_INDEX_INDEX);
                    continue;
                }
                /* Create new head index */
                /* Elements may have been read during resizing, so we need to re-adjust */

                final int oldHead = queueStart;
                final int updatedHead = headcurr ^ length;

                final int newHead = queueLength(oldHead, updatedHead, lengthMask) | expectedLength;

                Arrays.fill(newElements, 0, newHead, null); /* Remove read elements */

                /* Try to lock head */

                //if (!HEAD_INDEX_HANDLE.compareAndSet(this, headcurr, UINT_MAX)) {
                if (!INDICES_HANDLE.compareAndSet(this.indices, AVAILABLE_HEAD_INDEX_INDEX, headcurr, UINT_MAX)) {
                    ConcurrentUtil.pause();
                    continue;
                }

                /* Head is locked */
                /* Note that this is the only area where reads can block */
                this.elements = newElements;
                //AVAILABLE_TAIL_INDEX_HANDLE.set(this, requiredLength | expectedLength);
                INDICES_HANDLE.set(this.indices, AVAILABLE_TAIL_INDEX_INDEX, remainingLength | expectedLength);

                //ALLOCATED_TAIL_INDEX_HANDLE.setRelease(this, requiredLength | expectedLength);
                INDICES_HANDLE.setRelease(this.indices, requiredLength | expectedLength);

                //HEAD_INDEX_HANDLE.setVolatile(this, newHead);
                INDICES_HANDLE.setVolatile(this.indices, AVAILABLE_HEAD_INDEX_INDEX, newHead);
                /* Unlocked head */

                break;
            }
            return length - remainingLength;
        }
    }
}