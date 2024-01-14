package ca.spottedleaf.concurrentutil.collection;

import ca.spottedleaf.concurrentutil.util.ConcurrentUtil;
import ca.spottedleaf.concurrentutil.util.Validate;

import java.lang.invoke.VarHandle;
import java.util.ConcurrentModificationException;

/**
 * Single reader thread single writer thread queue. The reader side of the queue is ordered by acquire semantics,
 * and the writer side of the queue is ordered by release semantics.
 */
// TODO test
public class SRSWLinkedQueue<E> {

    // always non-null
    protected LinkedNode<E> head;

    // always non-null
    protected LinkedNode<E> tail;

    /* IMPL NOTE: Leave hashCode and equals to their defaults */

    public SRSWLinkedQueue() {
        final LinkedNode<E> dummy = new LinkedNode<>(null, null);
        this.head = this.tail = dummy;
    }

    /**
     * Must be the reader thread.
     *
     * <p>
     * Returns, without removing, the first element of this queue.
     * </p>
     * @return Returns, without removing, the first element of this queue.
     */
    public E peekFirst() {
        LinkedNode<E> head = this.head;
        E ret = head.getElementPlain();
        if (ret == null) {
            head = head.getNextAcquire();
            if (head == null) {
                // empty
                return null;
            }
            // update head reference for next poll() call
            this.head = head;
            // guaranteed to be non-null
            ret = head.getElementPlain();
            if (ret == null) {
                throw new ConcurrentModificationException("Multiple reader threads");
            }
        }

        return ret;
    }

    /**
     * Must be the reader thread.
     *
     * <p>
     * Returns and removes the first element of this queue.
     * </p>
     * @return Returns and removes the first element of this queue.
     */
    public E poll() {
        LinkedNode<E> head = this.head;
        E ret = head.getElementPlain();
        if (ret == null) {
            head = head.getNextAcquire();
            if (head == null) {
                // empty
                return null;
            }
            // guaranteed to be non-null
            ret = head.getElementPlain();
            if (ret == null) {
                throw new ConcurrentModificationException("Multiple reader threads");
            }
        }

        head.setElementPlain(null);
        LinkedNode<E> next = head.getNextAcquire();
        this.head = next == null ? head : next;

        return ret;
    }

    /**
     * Must be the writer thread.
     *
     * <p>
     * Adds the element to the end of the queue.
     * </p>
     *
     * @throws NullPointerException If the provided element is null
     */
    public void addLast(final E element) {
        Validate.notNull(element, "Provided element cannot be null");
        final LinkedNode<E> append = new LinkedNode<>(element, null);

        this.tail.setNextRelease(append);
        this.tail = append;
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

        protected final void setElementPlain(final E update) {
            ELEMENT_HANDLE.set(this, (Object)update);
        }
        /* next */

        @SuppressWarnings("unchecked")
        protected final LinkedNode<E> getNextPlain() {
            return (LinkedNode<E>)NEXT_HANDLE.get(this);
        }

        @SuppressWarnings("unchecked")
        protected final LinkedNode<E> getNextAcquire() {
            return (LinkedNode<E>)NEXT_HANDLE.getAcquire(this);
        }

        protected final void setNextPlain(final LinkedNode<E> next) {
            NEXT_HANDLE.set(this, next);
        }

        protected final void setNextRelease(final LinkedNode<E> next) {
            NEXT_HANDLE.setRelease(this, next);
        }
    }
}
