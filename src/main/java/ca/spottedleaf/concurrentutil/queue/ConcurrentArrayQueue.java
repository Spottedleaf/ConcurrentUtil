package ca.spottedleaf.concurrentutil.queue;

import ca.spottedleaf.concurrentutil.ConcurrentUtil;
import ca.spottedleaf.concurrentutil.util.IntegerUtil;


/** @deprecated until further revised */
@Deprecated
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
    private final int[] indices = new int[6 * (ConcurrentUtil.CACHE_LINE_SIZE / 4)];


    /**
     * The first element in this queue.
     */
    private static final int AVAILABLE_HEAD_INDEX        = 1 * (ConcurrentUtil.CACHE_LINE_SIZE / 4);

    /**
     *
     */
    private static final int ALLOCATED_HEAD_INDEX       = 2 * (ConcurrentUtil.CACHE_LINE_SIZE / 4);

    /**
     * The ready-to-read last element added to this queue.
     */
    private static final int AVAILABLE_TAIL_INDEX       = 3 * (ConcurrentUtil.CACHE_LINE_SIZE / 4);

    /**
     * The last element index allocated for writing.
     */
    private static final int ALLOCATED_TAIL_INDEX       = 4 * (ConcurrentUtil.CACHE_LINE_SIZE / 4);

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
}