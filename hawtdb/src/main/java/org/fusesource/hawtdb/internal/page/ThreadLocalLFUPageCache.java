package org.fusesource.hawtdb.internal.page;

import org.fusesource.hawtdb.util.LFUCache;

/**
 * @author Sergio Bossa
 */
public class ThreadLocalLFUPageCache<Integer, Value> implements PageCache<Integer, Value> {
    
    private final ThreadLocal<LFUCache<Integer, Value>> cache;

    public ThreadLocalLFUPageCache(final int maxCacheSize, final float evictionFactor) {
        cache = new ThreadLocal<LFUCache<Integer, Value>>() {

            @Override
            protected LFUCache<Integer, Value> initialValue() {
                return new LFUCache<Integer, Value>(maxCacheSize, evictionFactor);
            }
            
        };
    }

    public void put(Integer k, Value v) {
        cache.get().put(k, v);
    }

    public Value get(Integer k) {
        return cache.get().get(k);
    }

    public Value remove(Integer k) {
        return cache.get().remove(k);
    }

    public void clear() {
        cache.get().clear();
    }

    public int size() {
        return cache.get().size();
    }
}
