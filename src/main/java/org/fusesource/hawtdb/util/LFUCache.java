package org.fusesource.hawtdb.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author Sergio Bossa
 */
public class LFUCache<Key, Value> {

    private final Map<Key, CacheNode<Key, Value>> cache;
    private final Set[] frequencyList;
    private int lowestFrequency;
    private int maxFrequency;
    //
    private final int maxCacheSize;
    private final float evictionFactor;

    public LFUCache(int maxCacheSize, float evictionFactor) {
        if (evictionFactor <= 0 || evictionFactor >= 1) {
            throw new IllegalArgumentException("Eviction factor must be greater than 0 and lesser than or equal to 1");
        }
        this.cache = new HashMap<Key, CacheNode<Key, Value>>(maxCacheSize);
        this.frequencyList = new Set[maxCacheSize];
        this.lowestFrequency = 0;
        this.maxFrequency = maxCacheSize;
        this.maxCacheSize = maxCacheSize;
        this.evictionFactor = evictionFactor;
    }

    synchronized public void put(Key k, Value v) {
        if (!cache.containsKey(k)) {
            if (cache.size() == maxCacheSize) {
                doEviction();
            }
            CacheNode<Key, Value> cacheNode = new CacheNode(k, v, 0);
            Set<CacheNode<Key, Value>> nodes = frequencyList[0];
            if (nodes == null) {
                nodes = new HashSet<CacheNode<Key, Value>>();
                frequencyList[0] = nodes;
                lowestFrequency = 0;
            }
            nodes.add(cacheNode);
            cache.put(k, cacheNode);
        }
    }

    synchronized public Value get(Key k) {
        CacheNode<Key, Value> currentNode = cache.get(k);
        if (currentNode != null) {
            int oldFrequency = currentNode.frequency;
            if (oldFrequency < maxFrequency) {
                int nextFrequency = oldFrequency + 1;
                Set<CacheNode<Key, Value>> oldNodes = frequencyList[oldFrequency];
                Set<CacheNode<Key, Value>> newNodes = frequencyList[nextFrequency];
                if (newNodes == null) {
                    newNodes = new HashSet<CacheNode<Key, Value>>();
                    frequencyList[nextFrequency] = newNodes;
                }
                CacheNode<Key, Value> updatedNode = new CacheNode<Key, Value>(k, currentNode.v, nextFrequency);
                oldNodes.remove(currentNode);
                newNodes.add(updatedNode);
                cache.put(k, updatedNode);
                if (lowestFrequency == oldFrequency && oldNodes.isEmpty()) {
                    lowestFrequency = nextFrequency;
                    frequencyList[oldFrequency] = null;
                } else if (oldNodes.isEmpty()) {
                    frequencyList[oldFrequency] = null;
                }
            }
            return currentNode.v;
        } else {
            return null;
        }
    }

    synchronized public int frequencyOf(Key k) {
        CacheNode<Key, Value> node = cache.get(k);
        if (node != null) {
            return node.frequency + 1;
        } else {
            return 0;
        }
    }

    synchronized public int size() {
        return cache.size();
    }

    private void doEviction() {
        int currentlyDeleted = 0;
        float target = maxCacheSize * evictionFactor;
        while (currentlyDeleted < target) {
            Set<CacheNode<Key, Value>> nodes = frequencyList[lowestFrequency];
            if (nodes.isEmpty()) {
                throw new IllegalStateException("Lowest frequency constraint violated!");
            } else {
                Iterator<CacheNode<Key, Value>> it = nodes.iterator();
                while (it.hasNext() && currentlyDeleted++ < target) {
                    CacheNode<Key, Value> node = it.next();
                    it.remove();
                    cache.remove(node.k);
                }
                if (!it.hasNext()) {
                    frequencyList[lowestFrequency] = null;
                    while (frequencyList[lowestFrequency] == null) {
                        lowestFrequency++;
                    }
                }
            }
        }
    }

    private static class CacheNode<Key, Value> {

        public final Key k;
        public final Value v;
        public final int frequency;

        public CacheNode(Key k, Value v, int frequency) {
            this.k = k;
            this.v = v;
            this.frequency = frequency;
        }

    }
}
