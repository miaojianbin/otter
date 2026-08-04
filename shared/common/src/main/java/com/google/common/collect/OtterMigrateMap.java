package com.google.common.collect;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class OtterMigrateMap {

    public static <K, V> ConcurrentMap<K, V> makeComputingMap(MapMaker maker,
                                                              Function<? super K, ? extends V> computingFunction) {
        return makeComputingMap(computingFunction);
    }

    public static <K, V> ConcurrentMap<K, V> makeComputingMap(Function<? super K, ? extends V> computingFunction) {
        return computingMap(CacheBuilder.newBuilder(), computingFunction);
    }

    public static <K, V> ConcurrentMap<K, V> makeSoftValueComputingMap(MapMaker maker,
                                                                       Function<? super K, ? extends V> computingFunction) {
        return makeSoftValueComputingMap(computingFunction);
    }

    public static <K, V> ConcurrentMap<K, V> makeSoftValueComputingMap(Function<? super K, ? extends V> computingFunction) {
        return computingMap(CacheBuilder.newBuilder().softValues(), computingFunction);
    }

    public static <K, V> ConcurrentMap<K, V> makeSoftValueComputingMapWithTimeout(MapMaker maker,
                                                                                  Function<? super K, ? extends V> computingFunction,
                                                                                  long timeout, TimeUnit timeUnit) {
        return makeSoftValueComputingMapWithTimeout(computingFunction, timeout, timeUnit);
    }

    public static <K, V> ConcurrentMap<K, V> makeSoftValueComputingMapWithTimeout(Function<? super K, ? extends V> computingFunction,
                                                                       long timeout, TimeUnit timeUnit) {
        return computingMap(CacheBuilder.newBuilder().expireAfterWrite(timeout, timeUnit).softValues(),
                            computingFunction);
    }

    public static <K, V> ConcurrentMap<K, V> makeSoftValueMapWithTimeout(MapMaker maker,
    long timeout, TimeUnit timeUnit) {
        return makeSoftValueMapWithTimeout(timeout, timeUnit);
    }

    public static <K, V> ConcurrentMap<K, V> makeSoftValueMapWithTimeout(long timeout, TimeUnit timeUnit) {
        return CacheBuilder.newBuilder().expireAfterWrite(timeout, timeUnit).softValues().<K, V>build().asMap();
    }

    public static <K, V> ConcurrentMap<K, V> makeSoftValueComputingMapWithRemoveListenr(MapMaker maker,
                                                                                        Function<? super K, ? extends V> computingFunction,
                                                                                        final OtterRemovalListener listener) {
        return makeSoftValueComputingMapWithRemoveListenr(computingFunction, listener);
    }

    public static <K, V> ConcurrentMap<K, V> makeSoftValueComputingMapWithRemoveListenr(Function<? super K, ? extends V> computingFunction,
                                                                                        final OtterRemovalListener<K, V> listener) {
        CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().softValues().removalListener(new RemovalListener<Object, Object>() {

            @Override
            @SuppressWarnings("unchecked")
            public void onRemoval(RemovalNotification<Object, Object> notification) {
                if (notification == null) {
                    return;
                }

                listener.onRemoval((K) notification.getKey(), (V) notification.getValue());
            }
        });
        return computingMap(builder, computingFunction);
    }

    private static <K, V> ConcurrentMap<K, V> computingMap(CacheBuilder<Object, Object> builder,
                                                           final Function<? super K, ? extends V> computingFunction) {
        LoadingCache<K, V> cache = builder.build(new CacheLoader<K, V>() {

            @Override
            public V load(K key) {
                return computingFunction.apply(key);
            }
        });
        return new LoadingConcurrentMap<K, V>(cache);
    }

    private static class LoadingConcurrentMap<K, V> implements ConcurrentMap<K, V> {

        private final LoadingCache<K, V> cache;
        private final ConcurrentMap<K, V> delegate;

        LoadingConcurrentMap(LoadingCache<K, V> cache){
            this.cache = cache;
            this.delegate = cache.asMap();
        }

        @SuppressWarnings("unchecked")
        public V get(Object key) {
            return cache.getUnchecked((K) key);
        }

        public int size() {
            return delegate.size();
        }

        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        public boolean containsKey(Object key) {
            return delegate.containsKey(key);
        }

        public boolean containsValue(Object value) {
            return delegate.containsValue(value);
        }

        public V put(K key, V value) {
            return delegate.put(key, value);
        }

        public V remove(Object key) {
            return delegate.remove(key);
        }

        public void putAll(Map<? extends K, ? extends V> m) {
            delegate.putAll(m);
        }

        public void clear() {
            delegate.clear();
        }

        public Set<K> keySet() {
            return delegate.keySet();
        }

        public Collection<V> values() {
            return delegate.values();
        }

        public Set<Entry<K, V>> entrySet() {
            return delegate.entrySet();
        }

        public V putIfAbsent(K key, V value) {
            return delegate.putIfAbsent(key, value);
        }

        public boolean remove(Object key, Object value) {
            return delegate.remove(key, value);
        }

        public boolean replace(K key, V oldValue, V newValue) {
            return delegate.replace(key, oldValue, newValue);
        }

        public V replace(K key, V value) {
            return delegate.replace(key, value);
        }
    }

    public static interface OtterRemovalListener<K, V> {

        void onRemoval(K key, V value);
    }
}
