package ca.spottedleaf.concurrentutil.queue;

import ca.spottedleaf.concurrentutil.ConcurrentUtil;
import ca.spottedleaf.concurrentutil.util.Throw;
import ca.spottedleaf.concurrentutil.util.Validate;

import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;

/**
 * MT-Safe linked first in first out ordered queue.
 *
 * This queue should out-perform {@link java.util.concurrent.ConcurrentLinkedQueue} in high-contention reads/writes, and is
 * not any slower in lower contention reads/writes.
 * <p>
 * Note that this queue breaks the specification laid out by {@link Collection}, see {@link #preventAdds()} and {@link Collection#add(Object)}.
 * </p>
 * @param <E> Type of element in this queue.
 */
public class ConcurrentLinkedList<E> implements Queue<E> {

    @jdk.internal.vm.annotation.Contended
    protected volatile LinkedNode<E> head; /* Always non-null, high chance of being the actual head */

    @jdk.internal.vm.annotation.Contended
    protected volatile LinkedNode<E> tail; /* Always non-null, high chance of being the actual tail */

    /* Note that it is possible to reach head from tail. */

    /* IMPL NOTE: Leave hashCode and equals to their defaults */

    protected static final VarHandle HEAD_HANDLE = ConcurrentUtil.getVarHandle(ConcurrentLinkedList.class, "head", LinkedNode.class);
    protected static final VarHandle TAIL_HANDLE = ConcurrentUtil.getVarHandle(ConcurrentLinkedList.class, "tail", LinkedNode.class);

    /* head */

    protected final void setHeadPlain(final LinkedNode<E> newHead) {
        HEAD_HANDLE.set(this, newHead);
    }

    protected final void setHeadOpaque(final LinkedNode<E> newHead) {
        HEAD_HANDLE.setOpaque(this, newHead);
    }

    @SuppressWarnings("unchecked")
    protected final LinkedNode<E> getHeadPlain() {
        return (LinkedNode<E>)HEAD_HANDLE.get(this);
    }

    @SuppressWarnings("unchecked")
    protected final LinkedNode<E> getHeadOpaque() {
        return (LinkedNode<E>)HEAD_HANDLE.getOpaque(this);
    }

    @SuppressWarnings("unchecked")
    protected final LinkedNode<E> getHeadAcquire() {
        return (LinkedNode<E>)HEAD_HANDLE.getAcquire(this);
    }

    /* tail */

    protected final void setTailPlain(final LinkedNode<E> newTail) {
        TAIL_HANDLE.set(this, newTail);
    }

    protected final void setTailOpaque(final LinkedNode<E> newTail) {
        TAIL_HANDLE.setOpaque(this, newTail);
    }

    @SuppressWarnings("unchecked")
    protected final LinkedNode<E> getTailPlain() {
        return (LinkedNode<E>)TAIL_HANDLE.get(this);
    }

    @SuppressWarnings("unchecked")
    protected final LinkedNode<E> getTailOpaque() {
        return (LinkedNode<E>)TAIL_HANDLE.getOpaque(this);
    }
    /**
     * Constructs a {@code ConcurrentLinkedList}, initially empty.
     * <p>
     * The returned object may not be published without synchronization.
     * </p>
     */
    public ConcurrentLinkedList() {
        final LinkedNode<E> value = new LinkedNode<>(null, null);
        this.setHeadPlain(value);
        this.setTailPlain(value);
    }

    /**
     * Constructs a {@code ConcurrentLinkedList}, initially containing all elements in the specified {@code collection}.
     * <p>
     * The returned object may not be published without synchronization.
     * </p>
     * @param collection The specified collection.
     * @throws NullPointerException If {@code collection} is {@code null} or contains {@code null} elements.
     */
    public ConcurrentLinkedList(final Iterable<? extends E> collection) {
        final Iterator<? extends E> elements = collection.iterator();

        if (!elements.hasNext()) {
            final LinkedNode<E> value = new LinkedNode<>(null, null);
            this.setHeadPlain(value);
            this.setTailPlain(value);
            return;
        }

        final LinkedNode<E> head = new LinkedNode<>(Validate.notNull(elements.next(), "Null element"), null);
        LinkedNode<E> tail = head;

        while (elements.hasNext()) {
            final LinkedNode<E> next = new LinkedNode<>(Validate.notNull(elements.next(), "Null element"), null);
            tail.setNextPlain(next);
            tail = next;
        }

        this.setHeadPlain(head);
        this.setTailPlain(tail);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public E remove() throws NoSuchElementException {
        final E ret = this.poll();

        if (ret == null) {
            throw new NoSuchElementException();
        }

        return ret;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Contrary to the specification of {@link Collection#add}, this method will fail to add the element to this queue
     * and return {@code false} if this queue is add-blocked.
     * </p>
     */
    @Override
    public boolean add(final E element) {
        return this.offer(element);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public E element() throws NoSuchElementException {
        final E ret = this.peek();

        if (ret == null) {
            throw new NoSuchElementException();
        }

        return ret;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method may also return {@code false} to indicate an element was not added if this queue is add-blocked.
     * </p>
     */
    @Override
    public boolean offer(final E element) {
        Validate.notNull(element, "Null element");

        final LinkedNode<E> node = new LinkedNode<>(element, null);

        return this.appendList(node, node);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public E peek() {
        for (LinkedNode<E> head = this.getHeadOpaque(), curr = head;;) {
            final LinkedNode<E> next = curr.getNextVolatile();
            final E element = curr.getElementPlain(); /* Likely in sync */

            if (element != null) {
                if (this.getHeadOpaque() == head && curr != head) {
                    this.setHeadOpaque(curr);
                }
                return element;
            }

            if (next == null || curr == next) {
                return null;
            }
            curr = next;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public E poll() {
        return this.removeHead();
    }

    /**
     * Retrieves and removes the head of this queue if it matches the specified predicate. If this queue is empty
     * or the head does not match the predicate, this function returns {@code null}.
     * @param predicate The specified predicate.
     * @return The head if it matches the predicate, or {@code null} if it did not or this queue is empty.
     */
    public E pollIf(final Predicate<E> predicate) {
        return this.removeHead(Validate.notNull(predicate, "Null predicate"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        //noinspection StatementWithEmptyBody
        while (this.poll() != null);
    }

    /**
     * Prevents elements from being added to this queue. Once this is called, any attempt to add to this queue will fail.
     * <p>
     * This function is MT-Safe.
     * </p>
     * @return {@code true} if the queue was modified to prevent additions, {@code false} if it already prevented additions.
     */
    public boolean preventAdds() {
        final LinkedNode<E> deadEnd = new LinkedNode<>(null, null);
        deadEnd.setNextPlain(deadEnd);

        if (!this.appendList(deadEnd, deadEnd)) {
            return false;
        }

        this.setTailPlain(deadEnd); /* (try to) Ensure tail is set for the following #allowAdds call */
        return true;
    }

    /**
     * Allows elements to be added to this queue once again. Note that this function has undefined behaviour if
     * {@link #preventAdds()} is not called beforehand.
     * <p>
     * This function is not MT-Safe.
     * </p>
     */
    public void allowAdds() {
        LinkedNode<E> tail = this.getTailPlain();

        /* We need to find the tail given the cas on tail isn't atomic (nor volatile) in this.appendList */
        /* Thus it is possible for an outdated tail to be set */
        while (tail != (tail = tail.getNextPlain())) {}

        tail.setNextVolatile(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(final Object object) {
        Validate.notNull(object, "Null object to remove");

        for (LinkedNode<E> curr = this.getHeadOpaque();;) {
            final LinkedNode<E> next = curr.getNextVolatile();
            final E element = curr.getElementPlain(); /* Likely in sync */

            if (element != null) {
                if ((element == object || element.equals(object))
                        && curr.getAndSetElementVolatile(null) == element) {
                    return true;
                }
            }

            if (next == curr || next == null) {
                break;
            }
            curr = next;
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeIf(final Predicate<? super E> filter) {
        Validate.notNull(filter, "Null filter");

        boolean ret = false;

        for (LinkedNode<E> curr = this.getHeadOpaque();;) {
            final LinkedNode<E> next = curr.getNextVolatile();
            final E element = curr.getElementPlain(); /* Likely in sync */

            if (element != null) {
                ret |= filter.test(element) && curr.getAndSetElementVolatile(null) == element;
            }

            if (next == null || next == curr) {
                break;
            }
            curr = next;
        }

        return ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeAll(final Collection<?> collection) {
        Validate.notNull(collection, "Null collection");

        boolean ret = false;

        /* Volatile is required to synchronize with the write to the first element */
        for (LinkedNode<E> curr = this.getHeadOpaque();;) {
            final LinkedNode<E> next = curr.getNextVolatile();
            final E element = curr.getElementPlain(); /* Likely in sync */

            if (element != null) {
                ret |= collection.contains(element) && curr.getAndSetElementVolatile(null) == element;
            }

            if (next == null || next == curr) {
                break;
            }
            curr = next;
        }

        return ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean retainAll(final Collection<?> collection) {
        Validate.notNull(collection, "Null collection");

        boolean ret = false;

        for (LinkedNode<E> curr = this.getHeadOpaque();;) {
            final LinkedNode<E> next = curr.getNextVolatile();
            final E element = curr.getElementPlain(); /* Likely in sync */

            if (element != null) {
                ret |= !collection.contains(element) && curr.getAndSetElementVolatile(null) == element;
            }

            if (next == null || next == curr) {
                break;
            }
            curr = next;
        }

        return ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object[] toArray() {
        final List<E> ret = new ArrayList<>();

        for (LinkedNode<E> curr = this.getHeadOpaque();;) {
            final LinkedNode<E> next = curr.getNextVolatile();
            final E element = curr.getElementPlain(); /* Likely in sync */

            if (element != null) {
                ret.add(element);
            }

            if (next == null || next == curr) {
                break;
            }
            curr = next;
        }

        return ret.toArray();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T[] toArray(final T[] array) {
        final List<T> ret = new ArrayList<>();

        for (LinkedNode<E> curr = this.getHeadOpaque();;) {
            final LinkedNode<E> next = curr.getNextVolatile();
            final E element = curr.getElementPlain(); /* Likely in sync */

            if (element != null) {
                //noinspection unchecked
                ret.add((T) element);
            }

            if (next == null || next == curr) {
                break;
            }
            curr = next;
        }

        return ret.toArray(array);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T[] toArray(final IntFunction<T[]> generator) {
        Validate.notNull(generator, "Null generator");

        final List<T> ret = new ArrayList<>();

        for (LinkedNode<E> curr = this.getHeadOpaque();;) {
            final LinkedNode<E> next = curr.getNextVolatile();
            final E element = curr.getElementPlain(); /* Likely in sync */

            if (element != null) {
                //noinspection unchecked
                ret.add((T) element);
            }

            if (next == null || next == curr) {
                break;
            }
            curr = next;
        }

        return ret.toArray(generator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append("ConcurrentLinkedList: {elements: {");

        int deadEntries = 0;
        int totalEntries = 0;
        int aliveEntries = 0;

        boolean addLocked = false;

        for (LinkedNode<E> curr = this.getHeadOpaque();; ++totalEntries) {
            final LinkedNode<E> next = curr.getNextVolatile();
            final E element = curr.getElementPlain(); /* Likely in sync */

            if (element == null) {
                ++deadEntries;
            } else {
                ++aliveEntries;
            }

            if (totalEntries != 0) {
                builder.append(", ");
            }

            builder.append(totalEntries).append(": \"").append(element).append('"');

            if (next == null) {
                break;
            }
            if (curr == next) {
                addLocked = true;
                break;
            }
            curr = next;
        }

        builder.append("}, total_entries: \"").append(totalEntries).append("\", alive_entries: \"").append(aliveEntries)
                .append("\", dead_entries:").append(deadEntries).append("\", add_locked: \"").append(addLocked)
                .append("\"}");

        return builder.toString();
    }

    /**
     * Adds all elements from the specified collection to this queue. The addition is atomic.
     * @param collection The specified collection.
     * @return {@code true} if all elements were added successfully, or {@code false} if this queue is add-blocked, or
     * {@code false} if the specified collection contains no elements.
     */
    @Override
    public boolean addAll(final Collection<? extends E> collection) {
        return this.addAll((Iterable<? extends E>)collection);
    }

    /**
     * Adds all elements from the specified iterable object to this queue. The addition is atomic.
     * @param iterable The specified iterable object.
     * @return {@code true} if all elements were added successfully, or {@code false} if this queue is add-blocked, or
     * {@code false} if the specified iterable contains no elements.
     */
    public boolean addAll(final Iterable<? extends E> iterable) {
        Validate.notNull(iterable, "Null iterable");

        final Iterator<? extends E> elements = iterable.iterator();
        if (!elements.hasNext()) {
            return false;
        }

        /* Build a list of nodes to append */
        /* This is an much faster due to the fact that zero additional synchronization is performed */

        final LinkedNode<E> head = new LinkedNode<>(Validate.notNull(elements.next(), "Null element"), null);
        LinkedNode<E> tail = head;

        while (elements.hasNext()) {
            final LinkedNode<E> next = new LinkedNode<>(Validate.notNull(elements.next(), "Null element"), null);
            tail.setNextPlain(next);
            tail = next;
        }

        return this.appendList(head, tail);
    }

    /**
     * Adds all of the elements from the specified array to this queue.
     * @param items The specified array.
     * @return {@code true} if all elements were added successfully, or {@code false} if this queue is add-blocked, or
     * {@code false} if the specified array has a length of 0.
     */
    public boolean addAll(final E[] items) {
        return this.addAll(items, 0, items.length);
    }

    /**
     * Adds all of the elements from the specified array to this queue.
     * @param items The specified array.
     * @param off The offset in the array.
     * @param len The number of items.
     * @return {@code true} if all elements were added successfully, or {@code false} if this queue is add-blocked, or
     * {@code false} if the specified array has a length of 0.
     */
    public boolean addAll(final E[] items, final int off, final int len) {
        Validate.notNull(items, "Items may not be null");
        Validate.arrayBounds(off, len, items.length, "Items array indices out of bounds");

        if (len == 0) {
            return false;
        }

        final LinkedNode<E> head = new LinkedNode<>(Validate.notNull(items[off], "Null element"), null);
        LinkedNode<E> tail = head;

        for (int i = 1; i < len; ++i) {
            final LinkedNode<E> next = new LinkedNode<>(Validate.notNull(items[off + i], "Null element"), null);
            tail.setNextPlain(next);
            tail = next;
        }

        return this.appendList(head, tail);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsAll(final Collection<?> collection) {
        Validate.notNull(collection, "Null collection");

        for (final Object element : collection) {
            if (!this.contains(element)) {
                return false;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<E> iterator() {
        return new LinkedIterator<>(this.getHeadOpaque());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note that this function is computed non-atomically and in O(n) time. The value returned may not be representative of
     * the queue in its current state.
     * </p>
     */
    @Override
    public int size() {
        int size = 0;

        /* Volatile is required to synchronize with the write to the first element */
        for (LinkedNode<E> curr = this.getHeadOpaque();;) {
            final LinkedNode<E> next = curr.getNextVolatile();
            final E element = curr.getElementPlain(); /* Likely in sync */

            if (element != null) {
                ++size;
            }

            if (next == null || next == curr) {
                break;
            }
            curr = next;
        }

        return size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return this.peek() == null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(final Object object) {
        Validate.notNull(object, "Null object");

        for (LinkedNode<E> curr = this.getHeadOpaque();;) {
            final LinkedNode<E> next = curr.getNextVolatile();
            final E element = curr.getElementPlain(); /* Likely in sync */

            if (element != null && (element == object || element.equals(object))) {
                return true;
            }

            if (next == null || next == curr) {
                break;
            }
            curr = next;
        }

        return false;
    }

    /**
     * Finds the first element in this queue that matches the predicate.
     * @param predicate The predicate to test elements against.
     * @return The first element that matched the predicate, {@code null} if none matched.
     */
    public E find(final Predicate<E> predicate) {
        Validate.notNull(predicate, "Null predicate");

        for (LinkedNode<E> curr = this.getHeadOpaque();;) {
            final LinkedNode<E> next = curr.getNextVolatile();
            final E element = curr.getElementPlain(); /* Likely in sync */

            if (element != null && predicate.test(element)) {
                return element;
            }

            if (next == null || next == curr) {
                break;
            }
            curr = next;
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void forEach(final Consumer<? super E> action) {
        Validate.notNull(action, "Null action");

        for (LinkedNode<E> curr = this.getHeadOpaque();;) {
            final LinkedNode<E> next = curr.getNextVolatile();
            final E element = curr.getElementPlain(); /* Likely in sync */

            if (element != null) {
                action.accept(element);
            }

            if (next == null || next == curr) {
                break;
            }
            curr = next;
        }
    }

    protected final boolean appendList(final LinkedNode<E> head, final LinkedNode<E> tail) {
        int failures = 0;

        for (LinkedNode<E> currTail = this.getTailOpaque(), curr = currTail;;) {
            /* It has been experimentally shown that placing the read before the backoff results in significantly greater performance */
            /* It is likely due to a cache miss caused by another write to the next field */
            final LinkedNode<E> next = curr.getNextVolatile();

            for (int i = 0; i < failures; ++i) {
                ConcurrentUtil.pause();
            }

            if (next == null) {
                final LinkedNode<E> compared = curr.compreExchangeNextVolatile(null, head);

                if (compared == null) {
                    /* Added */
                    /* Avoid CASing on tail more than we need to */
                    /* CAS to avoid setting an out-of-date tail */
                    if (this.getTailOpaque() == currTail) {
                        this.setTailOpaque(tail);
                    }
                    return true;
                }

                ++failures;
                curr = compared;
                continue;
            } else if (next == curr) {
                /* Additions are stopped */
                return false;
            }

            if (curr == currTail) {
                /* Tail is likely not up-to-date */
                curr = next;
            } else {
                /* Try to update to tail */
                if (currTail == (currTail = this.getTailOpaque())) {
                    curr = next;
                } else {
                    curr = currTail;
                }
            }
        }
    }

    protected final E removeHead(final Predicate<E> predicate) {
        int failures = 0;
        for (LinkedNode<E> head = this.getHeadOpaque(), curr = head;;) {
            /* It has been experimentally shown that placing the reads before the backoff results in significantly greater performance */
            /* It is likely due to a cache miss caused by another write to the next field */
            final E currentVal = curr.getElementVolatile();
            final LinkedNode<E> next = curr.getNextOpaque();

            for (int i = 0; i < failures; ++i) {
                ConcurrentUtil.pause();
            }

            if (currentVal != null) {
                if (!predicate.test(currentVal)) {
                    /* Try to update stale head */
                    if (curr != head && this.getHeadOpaque() == head) {
                        this.setHeadOpaque(curr);
                    }
                    return null;
                }
                if (curr.compareExchangeElementVolatile(currentVal, null) != currentVal) {
                    /* Failed to get head */
                    if (curr == (curr = next) || next == null) {
                        return null;
                    }
                    ++failures;
                    continue;
                }

                /* "CAS" to avoid setting an out-of-date head */
                if (this.getHeadOpaque() == head) {
                    this.setHeadOpaque(next != null ? next : curr);
                }

                return currentVal;
            }

            if (next == null) {
                /* Try to update stale head */
                if (curr != head && this.getHeadOpaque() == head) {
                    this.setHeadOpaque(curr);
                }
                return null; /* End of queue */
            }

            if (head == curr) {
                /* head is likely not up-to-date */
                curr = next;
            } else {
                /* Try to update to head */
                if (head == (head = this.getHeadOpaque())) {
                    curr = next;
                } else {
                    curr = head;
                }
            }
        }
    }

    protected final E removeHead() {
        int failures = 0;
        for (LinkedNode<E> head = this.getHeadOpaque(), curr = head;;) {
            final E currentVal = curr.getElementVolatile();
            final LinkedNode<E> next = curr.getNextOpaque();

            for (int i = 0; i < failures; ++i) {
                ConcurrentUtil.pause();
            }

            if (currentVal != null) {
                if (curr.compareExchangeElementVolatile(currentVal, null) != currentVal) {
                    /* Failed to get head */
                    if (curr == (curr = next) || next == null) {
                        return null;
                    }
                    ++failures;
                    continue;
                }

                /* "CAS" to avoid setting an out-of-date head */
                if (this.getHeadOpaque() == head) {
                    this.setHeadOpaque(next != null ? next : curr);
                }

                return currentVal;
            }

            if (next == null) {
                /* Try to update stale head */
                if (curr != head && this.getHeadOpaque() == head) {
                    this.setHeadOpaque(curr);
                }
                return null; /* End of queue */
            }

            if (head == curr) {
                /* head is likely not up-to-date */
                curr = next;
            } else {
                /* Try to update to head */
                if (head == (head = this.getHeadOpaque())) {
                    curr = next;
                } else {
                    curr = head;
                }
            }
        }
    }

    /**
     * Empties the queue into the specified consumer. This function is optimized for single-threaded reads, and should
     * be faster than a loop on {@link #poll()}.
     * <p>
     * This function is not MT-Safe. This function cannot be called with other read operations ({@link #peek()}, {@link #poll()},
     * {@link #clear()}, etc).
     * Write operations are safe to be called concurrently.
     * </p>
     * @param consumer The consumer to accept the elements.
     * @return The total number of elements drained.
     */
    public int drain(final Consumer<E> consumer) {
        return this.drain(consumer, false, Throw::rethrow);
    }

    /**
     * Empties the queue into the specified consumer. This function is optimized for single-threaded reads, and should
     * be faster than a loop on {@link #poll()}.
     * <p>
     * If {@code preventAdds} is {@code true}, then after this function returns the queue is guaranteed to be empty and
     * additions to the queue will fail.
     * </p>
     * <p>
     * This function is not MT-Safe. This function cannot be called with other read operations ({@link #peek()}, {@link #poll()},
     * {@link #clear()}, etc).
     * Write operations are safe to be called concurrently.
     * </p>
     * @param consumer The consumer to accept the elements.
     * @param preventAdds Whether to prevent additions to this queue after draining.
     * @return The total number of elements drained.
     */
    public int drain(final Consumer<E> consumer, final boolean preventAdds) {
        return this.drain(consumer, preventAdds, Throw::rethrow);
    }

    /**
     * Empties the queue into the specified consumer. This function is optimized for single-threaded reads, and should
     * be faster than a loop on {@link #poll()}.
     * <p>
     * If {@code preventAdds} is {@code true}, then after this function returns the queue is guaranteed to be empty and
     * additions to the queue will fail.
     * </p>
     * <p>
     * This function is not MT-Safe. This function cannot be called with other read operations ({@link #peek()}, {@link #poll()},
     * {@link #clear()}, {@link #remove(Object)} etc).
     * Only write operations are safe to be called concurrently.
     * </p>
     * @param consumer The consumer to accept the elements.
     * @param preventAdds Whether to prevent additions to this queue after draining.
     * @param exceptionHandler Invoked when the consumer raises an exception.
     * @return The total number of elements drained.
     */
    public int drain(final Consumer<E> consumer, final boolean preventAdds, final Consumer<Throwable> exceptionHandler) {
        Validate.notNull(consumer, "Null consumer");
        Validate.notNull(exceptionHandler, "Null exception handler");

        /* This function assumes proper synchronization is made to ensure drain and no other read function are called concurrently */
        /* This allows plain write usages instead of opaque or higher */
        int total = 0;

        final LinkedNode<E> head = this.getHeadAcquire(); /* Required to synchronize with the write to the first element field */
        LinkedNode<E> curr = head;

        for (;;) {
            /* Volatile acquires with the write to the element field */
            final E currentVal = curr.getElementPlain();

            LinkedNode<E> next = curr.getNextVolatile();

            if (next == curr) {
                /* Add-locked nodes always have a null value */
                break;
            }

            if (currentVal == null) {
                if (next == null) {
                    break;
                }
                curr = next;
                continue;
            }

            try {
                consumer.accept(currentVal);
            } catch (final Exception ex) {
                this.setHeadOpaque(next != null ? next : curr); /* Avoid perf penalty (of reiterating) if the exception handler decides to re-throw */
                curr.setElementOpaque(null); /* set here, we might re-throw */

                exceptionHandler.accept(ex);
            }

            curr.setElementOpaque(null);


            ++total;

            if (next == null) {
                if (preventAdds) {
                    if ((next = curr.compreExchangeNextVolatile(null, curr)) != null) {
                        /* Retry with next value */
                        curr = next;
                        continue;
                    }
                }
                break;
            }

            curr = next;
        }
        if (curr != head) {
            this.setHeadOpaque(curr); /* While this may be a plain write, eventually publish it for methods such as find. */
        }
        return total;
    }

    @Override
    public Spliterator<E> spliterator() { // TODO implement
        return Spliterators.spliterator(this, Spliterator.CONCURRENT |
                Spliterator.NONNULL | Spliterator.ORDERED);
    }

    protected static final class LinkedNode<E> {

        protected volatile Object element;
        protected volatile LinkedNode<E> next;

        protected static final VarHandle ELEMENT_HANDLE = ConcurrentUtil.getVarHandle(LinkedNode.class, "element", Object.class);
        protected static final VarHandle NEXT_HANDLE = ConcurrentUtil.getVarHandle(LinkedNode.class, "next", LinkedNode.class);

        protected LinkedNode(final Object element, final LinkedNode<E> next) {
            ELEMENT_HANDLE.set(this, element);
            NEXT_HANDLE.set(this, next);
        }

        /* element */

        @SuppressWarnings("unchecked")
        protected final E getElementPlain() {
            return (E)ELEMENT_HANDLE.get(this);
        }

        @SuppressWarnings("unchecked")
        protected final E getElementVolatile() {
            return (E)ELEMENT_HANDLE.getVolatile(this);
        }

        protected final void setElementPlain(final E update) {
            ELEMENT_HANDLE.set(this, (Object)update);
        }

        protected final void setElementOpaque(final E update) {
            ELEMENT_HANDLE.setOpaque(this, (Object)update);
        }

        protected final void setElementVolatile(final E update) {
            ELEMENT_HANDLE.setVolatile(this, (Object)update);
        }

        @SuppressWarnings("unchecked")
        protected final E getAndSetElementVolatile(final E update) {
            return (E)ELEMENT_HANDLE.getAndSet(this, update);
        }

        @SuppressWarnings("unchecked")
        protected final E compareExchangeElementVolatile(final E expect, final E update) {
            return (E)ELEMENT_HANDLE.compareAndExchange(this, expect, update);
        }

        /* next */

        @SuppressWarnings("unchecked")
        protected final LinkedNode<E> getNextPlain() {
            return (LinkedNode<E>)NEXT_HANDLE.get(this);
        }

        @SuppressWarnings("unchecked")
        protected final LinkedNode<E> getNextOpaque() {
            return (LinkedNode<E>)NEXT_HANDLE.getOpaque(this);
        }

        @SuppressWarnings("unchecked")
        protected final LinkedNode<E> getNextAcquire() {
            return (LinkedNode<E>)NEXT_HANDLE.getAcquire(this);
        }

        @SuppressWarnings("unchecked")
        protected final LinkedNode<E> getNextVolatile() {
            return (LinkedNode<E>)NEXT_HANDLE.getVolatile(this);
        }

        protected final void setNextPlain(final LinkedNode<E> next) {
            NEXT_HANDLE.set(this, next);
        }

        protected final void setNextVolatile(final LinkedNode<E> next) {
            NEXT_HANDLE.setVolatile(this, next);
        }

        @SuppressWarnings("unchecked")
        protected final LinkedNode<E> compreExchangeNextVolatile(final LinkedNode<E> expect, final LinkedNode<E> set) {
            return (LinkedNode<E>)NEXT_HANDLE.compareAndExchange(this, expect, set);
        }
    }

    protected static final class LinkedIterator<E> implements Iterator<E> {

        protected LinkedNode<E> curr; /* last returned by next() */
        protected LinkedNode<E> next; /* next to return from next() */
        protected E nextElement; /* cached to avoid a race condition with removing or polling */

        protected LinkedIterator(final LinkedNode<E> start) {
            /* setup nextElement and next */
            for (LinkedNode<E> curr = start;;) {
                final LinkedNode<E> next = curr.getNextVolatile();

                final E element = curr.getElementPlain();

                if (element != null) {
                    this.nextElement = element;
                    this.next = curr;
                    break;
                }

                if (next == null || next == curr) {
                    break;
                }
                curr = next;
            }
        }

        protected final void findNext() {
            /* only called if this.nextElement != null, which means this.next != null */
            for (LinkedNode<E> curr = this.next;;) {
                final LinkedNode<E> next = curr.getNextVolatile();

                if (next == null || next == curr) {
                    break;
                }

                final E element = next.getElementPlain();

                if (element != null) {
                    this.nextElement = element;
                    this.curr = this.next; /* this.next will be the value returned from next(), set this.curr for remove() */
                    this.next = next;
                    return;
                }
                curr = next;
            }

            /* out of nodes to iterate */
            /* keep curr for remove() calls */
            this.next = null;
            this.nextElement = null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return this.nextElement != null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public E next() {
            final E element = this.nextElement;

            if (element == null) {
                throw new NoSuchElementException();
            }

            this.findNext();

            return element;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void remove() {
            if (this.curr == null) {
                throw new IllegalStateException();
            }

            this.curr.setElementVolatile(null);
            this.curr = null;
        }
    }
}