package io.github.spottedleaf.concurrentutil.map;

import io.github.spottedleaf.concurrentutil.ConcurrentUtil;

import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Concurrent hash table implementation which is copied for each write. Note that preemptive copying may occur in some
 * methods, such as {@link #compute(Object, BiFunction)} or {@link #replaceAll(BiFunction)}.
 * @param <K> Type of the keys.
 * @param <V> Type of the values mapped by keys.
 */
public class CopyOnWriteHashMap<K, V> implements ConcurrentMap<K, V> {

    protected volatile HashMap<K, V> map;
    protected final float loadFactor;

    protected static final VarHandle MAP_HANDLE = ConcurrentUtil.getVarHandle(CopyOnWriteHashMap.class, "map", HashMap.class);

    @SuppressWarnings("unchecked")
    protected final HashMap<K, V> getMapPlain() {
        return (HashMap<K, V>)MAP_HANDLE.get(this);
    }

    /**
     * Constructs this map with a capacity of {@code 16} and load factor of {@code 0.75f}.
     */
    public CopyOnWriteHashMap() {
        this(16, 0.75f);
    }

    /**
     * Constructs this map with the specified capacity and specified load factor.
     * @param capacity initial capacity, >= 0
     * @param loadFactor Load factor, > 0
     */
    public CopyOnWriteHashMap(final int capacity, final float loadFactor) {
        this.map = new HashMap<>(capacity, loadFactor);
        this.loadFactor = loadFactor;
    }

    /* no copy */

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return this.map.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(final Object key) {
        return this.map.containsKey(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(final Object value) {
        return this.map.containsValue(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(final Object key) {
        return this.map.get(key);
    }

    /**
     * <p>
     * The returned collection is unmodifiable and represents a snapshot of this map during some point in
     * this method call.
     * </p>
     */
    @Override
    public Collection<V> values() {
        return Collections.unmodifiableCollection(this.map.values());
    }

    /**
     * <p>
     * The returned collection is unmodifiable and represents a snapshot of this map during some point in
     * this method call.
     * </p>
     */
    @Override
    public Set<K> keySet() {
        return Collections.unmodifiableSet(this.map.keySet());
    }

    /**
     * <p>
     * The returned collection is unmodifiable and represents a snapshot of this map during some point in
     * this method call.
     * </p>
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        return Collections.unmodifiableSet(this.map.entrySet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object obj) {
        return this.map.equals(obj);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return this.map.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V getOrDefault(final Object key, final V defaultValue) {
        return this.map.getOrDefault(key, defaultValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void forEach(final BiConsumer<? super K, ? super V> action) {
        this.map.forEach(action);
    }

    /* copy */

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        synchronized (this) {
            //noinspection unchecked
            final HashMap<K, V> clone = (HashMap<K, V>) this.getMapPlain().clone();
            clone.clear();
            this.map = clone;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V put(final K key, final V value) {
        final V ret;
        synchronized (this) {
            //noinspection unchecked
            final HashMap<K, V> clone = (HashMap<K, V>) this.getMapPlain().clone();
            ret = clone.put(key, value);
            this.map = clone;
        }
        return ret;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This operation is performed atomically.
     * </p>
     */
    @Override
    public void putAll(final Map<? extends K, ? extends V> map) {
        if (map.isEmpty()) {
            return;
        }
        synchronized (this) {
            //noinspection unchecked
            final HashMap<K, V> clone = (HashMap<K, V>) this.getMapPlain().clone();

            for (final Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                try {
                    clone.put(entry.getKey(), entry.getValue());
                } catch (final IllegalStateException ignore) {} // iff an entry in the map param is removed concurrently
            }

            this.map = clone;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V putIfAbsent(final K key, final V value) {
        return this.computeIfAbsent(key, (final K k) -> value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("SuspiciousMethodCalls")
    public V remove(final Object key) {
        final V ret;
        synchronized (this) {
            final HashMap<K, V> map = this.getMapPlain();
            if (!map.containsKey(key)) {
                return null;
            }
            //noinspection unchecked
            final HashMap<K, V> clone = (HashMap<K, V>) map.clone();
            ret = clone.remove(key);
            this.map = clone;
        }
        return ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("SuspiciousMethodCalls")
    public boolean remove(final Object key, final Object value) {
        synchronized (this) {
            final HashMap<K, V> map = this.getMapPlain();
            final V current = map.get(key);

            /* defensive equals */
            if (current != value && (current != null && current.equals(value))) {
                return false;
            }

            //noinspection unchecked
            final HashMap<K, V> clone = (HashMap<K, V>) map.clone();
            clone.remove(key);
            this.map = map;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        synchronized (this) {
            final HashMap<K, V> map = this.getMapPlain();
            final V current = map.get(key);

            /* defensive equals */
            if (current != oldValue && (current != null && current.equals(oldValue))) {
                return false;
            }

            //noinspection unchecked
            final HashMap<K, V> clone = (HashMap<K, V>) map.clone();
            clone.put(key, newValue);
            this.map = clone;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V replace(final K key, final V value) {
        final V ret;
        synchronized (this) {
            final HashMap<K, V> map = this.getMapPlain();
            if (!map.containsKey(key)) {
                return null;
            }

            //noinspection unchecked
            final HashMap<K, V> clone = (HashMap<K, V>) map.clone();
            ret = clone.put(key, value);
            this.map = clone;
        }
        return ret;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This operation is performed atomically.
     * </p>
     */
    @Override
    public void replaceAll(final BiFunction<? super K, ? super V, ? extends V> function) {
        synchronized (this) {
            final HashMap<K, V> map = this.getMapPlain();
            final HashMap<K, V> clone = new HashMap<>(map.size(), this.loadFactor);
            for (final Map.Entry<K, V> entry : map.entrySet()) {
                final K key = entry.getKey();
                final V newValue = function.apply(key, entry.getValue());

                clone.put(key, newValue);
            }
            this.map = clone;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * This operation is performed atomically, and the mappingFunction will only be invoked once at most.
     * </p>
     */
    @Override
    public V computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) {
        final V ret;
        synchronized (this) {
            final HashMap<K, V> map = this.getMapPlain();

            final V curr = map.get(key);

            if (curr != null) {
                return curr;
            }

            ret = mappingFunction.apply(key);

            if (ret == null) {
                return null;
            }

            //noinspection unchecked
            final HashMap<K, V> clone = (HashMap<K, V>)map.clone();
            clone.put(key, ret);
            this.map = clone;
        }
        return ret;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This operation is performed atomically, and the remappingFunction will be invoked once at most.
     * </p>
     */
    @Override
    public V computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        final V ret;
        synchronized (this) {
            final HashMap<K, V> map = this.getMapPlain();

            final V previous = map.get(key);

            if (previous == null) {
                return null;
            }

            ret = remappingFunction.apply(key, previous);

            //noinspection unchecked
            final HashMap<K, V> clone = (HashMap<K, V>)map.clone();

            if (ret == null) {
                clone.remove(key);
            } else {
                clone.put(key, ret);
            }

            this.map = clone;
        }
        return ret;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This operation is performed atomically, and the remappingFunction will only be invoked once at most.
     * </p>
     */
    @Override
    public V compute(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        final V ret;
        synchronized (this) {
            final HashMap<K, V> map = this.getMapPlain();

            final V previous = map.get(key);

            ret = remappingFunction.apply(key, previous);

            //noinspection unchecked
            final HashMap<K, V> clone = (HashMap<K, V>)map.clone();

            if (ret == null) {
                clone.remove(key);
            } else {
                clone.put(key, ret);
            }

            this.map = clone;
        }
        return ret;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This operation is performed atomically, and the remappingFunction will only be invoked once at most.
     * </p>
     */
    @Override
    public V merge(final K key, final V value, final BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        final V ret;
        synchronized (this) {
            final HashMap<K, V> map = this.getMapPlain();

            final V previous = map.get(key);

            if (previous == null) {
                //noinspection unchecked
                final HashMap<K, V> clone = (HashMap<K, V>)map.clone();
                clone.put(key, value);
                this.map = map;

                return value;
            }

            ret = remappingFunction.apply(previous, value);

            //noinspection unchecked
            final HashMap<K, V> clone = (HashMap<K, V>)map.clone();

            if (ret == null) {
                clone.remove(key);
            } else {
                clone.put(key, ret);
            }
        }
        return ret;
    }
}