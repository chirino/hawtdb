package org.fusesource.hawtdb.internal.page;

/**
 * @author Sergio Bossa
 */
public class NoOpPageCache<Integer, Value> implements PageCache<Integer, Value> {
    
    public void put(Integer k, Value v) {
    }

    public Value get(Integer k) {
        return null;
    }

    public Value remove(Integer k) {
        return null;
    }

    public void clear() {
    }

    public int size() {
        return 0;
    }
}
