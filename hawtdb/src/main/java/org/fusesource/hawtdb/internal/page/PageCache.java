package org.fusesource.hawtdb.internal.page;

/**
 * @author Sergio Bossa
 */
public interface PageCache<Integer, Value> {
    
    public void put(Integer k, Value v);
    
    public Value get(Integer k);
    
    public Value remove(Integer k);
    
    public void clear();
    
    public int size();
}
