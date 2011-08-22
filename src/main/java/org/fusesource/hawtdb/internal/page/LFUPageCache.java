package org.fusesource.hawtdb.internal.page;

import org.fusesource.hawtdb.util.LFUCache;

/**
 * @author Sergio Bossa
 */
public class LFUPageCache<Integer, Value> implements PageCache<Integer, Value> {
    
    private final LFUCache<Integer, Value> cache;

    public LFUPageCache(int maxCacheSize, float evictionFactor) {
        cache = new LFUCache<Integer, Value>(maxCacheSize, evictionFactor);
    }

    public void put(Integer k, Value v) {
        cache.put(k, v);
    }

    public Value get(Integer k) {
        return cache.get(k);
    }

    public Value remove(Integer k) {
        return cache.remove(k);
    }

    public void clear() {
        cache.clear();
    }

    public int size() {
        return cache.size();
    }
}
