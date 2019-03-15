package ca.spottedleaf.concurrentutil.queue;

import ca.spottedleaf.concurrentutil.ConcurrentUtil;
import ca.spottedleaf.concurrentutil.util.CollectionUtil;
import ca.spottedleaf.concurrentutil.util.IntegerUtil;

import java.lang.invoke.VarHandle;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.Predicate;

public abstract class ConcurrentArrayQueue<E> implements Queue<E> { // TODO abstract is temporary

    public static final int NORESIZE_FLAG = (1 << 0);
    public static final int PEEK_FLAG     = (1 << 1);

    protected static final int UINT_MAX = -1;
    protected static final int MAX_INDEX = UINT_MAX >>> 2; /* 1 bit for encoded length, 1 bit for resize flag */
    protected static final int MAX_LENGTH = MAX_INDEX + 1;
    protected static final int INDEX_LENGTH_MASK = MAX_INDEX | MAX_LENGTH;

    protected static final int READING_BIT    = Integer.MIN_VALUE;
    protected static final int WRITING_BIT    = Integer.MIN_VALUE;
    protected static final int RESIZING_VALUE = UINT_MAX;

    @jdk.internal.vm.annotation.Contended
    protected volatile int headIndex;

    @jdk.internal.vm.annotation.Contended
    protected volatile int tailIndex;

    protected Object[] elements; /* access synchronized through indices */

    protected static final VarHandle HEAD_INDEX_HANDLE = ConcurrentUtil.getVarHandle(ConcurrentArrayQueue.class, "headIndex", int.class);
    protected static final VarHandle TAIL_INDEX_HANDLE = ConcurrentUtil.getVarHandle(ConcurrentArrayQueue.class, "tailIndex", int.class);

    /* head index */

    protected final int getHeadIndexPlain() {
        return (int)HEAD_INDEX_HANDLE.get(this);
    }

    protected final void setHeadIndexPlain(final int value) {
        HEAD_INDEX_HANDLE.set(this, value);
    }

    /* tail index */

    protected final int getTailIndexVolatile() {
        return (int)TAIL_INDEX_HANDLE.get(this);
    }

    protected final void setTailIndexPlain(final int value) {
        TAIL_INDEX_HANDLE.setOpaque(this, value);
    }

    public ConcurrentArrayQueue() {
        this(32);
    }

    public ConcurrentArrayQueue(int capacity) {
        if (capacity < 0 || capacity > MAX_LENGTH) {
            throw new IllegalArgumentException("Capacity " + capacity + " is out of bounds");
        } else if (capacity <= 32) {
            capacity = 32;
        } else {
            capacity = IntegerUtil.roundCeilLog2(capacity);
        }

        this.elements = new Object[capacity];
        this.setHeadIndexPlain(capacity);
        this.setTailIndexPlain(capacity);
        VarHandle.storeStoreFence();
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

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean offer(final E element) {
        this.add(element, 0);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean add(final E element) {
        this.add(element, 0);
        return true;
    }

    public boolean add(final E element, final int flags) {
        return false; // TODO implement
    }

    public boolean add(final E[] elements, final int flags) {
        return this.add(elements, 0, elements.length, flags);
    }

    public boolean add(final E[] elements, final int off, final int len, final int flags) {
        return false; // TODO implement
    }

    @Override
    public boolean addAll(final Collection<? extends E> collection) {
        return false; // TODO implement
    }

    @Override
    public E poll() {
        return null; // TODO implement
    }

    @Override
    public E peek() {
        return null; // TODO implement
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public E remove() throws NoSuchElementException {
        final E head = this.poll();
        if (head == null) {
            throw new NoSuchElementException();
        }
        return head;
    }

    @Override
    public E element() throws NoSuchElementException {
        final E head = this.peek();
        if (head == null) {
            throw new NoSuchElementException();
        }
        return head;
    }

    @Override
    public boolean remove(final Object object) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(final Collection<?> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(final Predicate<? super E> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return CollectionUtil.toString(this, "ConcurrentArrayQueue");
    }

    // TODO splitterator
}