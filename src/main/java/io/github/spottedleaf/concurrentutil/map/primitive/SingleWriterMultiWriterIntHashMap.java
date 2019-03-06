package io.github.spottedleaf.concurrentutil.map.primitive;

import io.github.spottedleaf.concurrentutil.ConcurrentUtil;
import io.github.spottedleaf.concurrentutil.lock.WeakSeqLock;
import io.github.spottedleaf.concurrentutil.util.ArrayUtil;

import java.lang.invoke.VarHandle;

public class SingleWriterMultiWriterIntHashMap<V> {

    protected final WeakSeqLock mapLock = new WeakSeqLock();

    /* inlined set of keys */
    /* the index at which a key resides in is also the index of its value */
    protected int[] keys;

    protected V[] values;

    private V zeroKeyValue;

    /** size excluding zero key value */
    private int size;

    protected static final VarHandle KEYS_HANDLE =
            ConcurrentUtil.getVarHandle(SingleWriterMultiWriterIntHashMap.class, "keys", int[].class);

    protected static final VarHandle VALUES_HANDLE =
            ConcurrentUtil.getVarHandle(SingleWriterMultiWriterIntHashMap.class, "values", Object[].class);

    protected static final VarHandle SIZE_HANDLE =
            ConcurrentUtil.getVarHandle(SingleWriterMultiWriterIntHashMap.class, "size", int.class);

    protected static final VarHandle ZERO_KEY_VALUE =
            ConcurrentUtil.getVarHandle(SingleWriterMultiWriterIntHashMap.class, "zeroKeyValue", Object.class);

    /* keys */

    protected int[] getKeysPlain() {
        return (int[])KEYS_HANDLE.get(this);
    }

    protected int[] getKeysAcquire() {
        return (int[])KEYS_HANDLE.getAcquire(this);
    }

    /* values */

    @SuppressWarnings("unchecked")
    protected V[] getValuesPlain() {
        return (V[])VALUES_HANDLE.get(this);
    }

    @SuppressWarnings("unchecked")
    protected V[] getValuesAcquire() {
        return (V[])VALUES_HANDLE.getAcquire(this);
    }

    /* size */

    protected int getSizePlain() {
        return (int)SIZE_HANDLE.get(this);
    }

    protected int getSizeOpaque() {
        return (int)SIZE_HANDLE.getOpaque(this);
    }

    /* zero key value */

    @SuppressWarnings("unchecked")
    protected V getZeroKeyValuePlain() {
        final Object ret = ZERO_KEY_VALUE.get(this);
        return (V)ret;
    }

    @SuppressWarnings("unchecked")
    protected V getZeroKeyValueOpaque() {
        final Object ret = ZERO_KEY_VALUE.getOpaque(this);
        return (V)ret;
    }

    @SuppressWarnings("unchecked")
    protected V getZeroKeyValueAcquire() {
        final Object ret = ZERO_KEY_VALUE.getAcquire(this);
        return (V)ret;
    }

    protected void setZeroKeyValuePlain(final V value) {
        ZERO_KEY_VALUE.set(this, (Object)value);
    }

    protected void setZeroKeyValueRelease(final V value) {
        ZERO_KEY_VALUE.setRelease(this, (Object)value);
    }

    protected int hash(final int value) {
        return value;
    }

    public int size() {
        if (this.getZeroKeyValueOpaque() != null) {
            return this.getSizeOpaque() + 1;
        } else {
            return this.getSizeOpaque();
        }
    }

    public V getValue(final int key) {
        if (key == 0) {
            return this.getZeroKeyValueAcquire();
        }

        int[] keys;
        V[] values;
        int capacity;
        int capacityMask;

        for (;;) {
            keys = this.getKeysPlain();
            values = this.getValuesAcquire();
            capacity = keys.length;
            capacityMask = capacity - 1;
            if (capacity != values.length) {
                break;
            }
        }

        final int hash = this.hash(key);
        final int index = hash & capacityMask; // optimized hash % capacity (when capacity is a power of 2)

        int readLock;
        int currIndex;

search_loop:
        for (;;) {
            readLock = this.mapLock.acquireRead();
            for (currIndex = index;;) {
                final int nextIndex = (currIndex + 1) & capacityMask;
                if (nextIndex == index) {
                    /* Note: This logic restricts min table size to 2. */

                    if (!this.mapLock.checkRead(readLock)) {
                        continue search_loop;
                    }
                }

                final int currKey = ArrayUtil.getOpaque(keys, currIndex);

                if (currKey == 0) {
                    if (!this.mapLock.checkRead(readLock)) {
                        continue search_loop;
                    }
                    return null;
                }

                if (key != currKey) {
                    currIndex = nextIndex;
                    continue;
                }

                /* it's important the value read is before the lock. */
                final V value = ArrayUtil.getAcquire(values, currIndex);
                if (!this.mapLock.checkRead(readLock)) {
                    continue search_loop;
                }

                return value;
            }
        }
    }

    public V getValueRelaxed(final int key) {

    }

    public V putValue(final int key, final V value) {
        if (key == 0) {
            final V prev = this.getZeroKeyValuePlain();
            this.setZeroKeyValueRelease(value);
            return prev;
        }

        final int[] keys = this.getKeysPlain();
        final V[] values = this.getValuesPlain();

    }
}