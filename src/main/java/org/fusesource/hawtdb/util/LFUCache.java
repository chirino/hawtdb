package org.fusesource.hawtdb.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * LFU cache implementation based on http://dhruvbird.com/lfu.pdf, with some notable differences:
 * <ul>
 * <li>
 * Frequency list is stored as an array with no next/prev pointers between nodes: looping over the array should be faster and more CPU-cache friendly than
 * using an ad-hoc linked-pointers structure.
 * </li>
 * <li>
 * The max frequency is capped at the cache size to avoid creating more and more frequency list entries, and all elements residing in the max frequency entry 
 * are re-positioned in the frequency entry linked set in order to put most recently accessed elements ahead of less recently ones, 
 * which will be collected sooner.
 * </li>
 * <li>
 * The eviction factor determines how many elements (more specifically, the percentage of) will be evicted.
 * </li>
 * </ul>
 * As a consequence, this cache runs in *amortized* O(1) time (considering the worst case of having the lowest frequency at 0 and having to evict all
 * elements).
 * 
 * @author Sergio Bossa
 */
public class LFUCache<Key, Value> {

    private final Map<Key, CacheNode<Key, Value>> cache;
    private final LinkedHashSet[] frequencyList;
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
        this.frequencyList = new LinkedHashSet[maxCacheSize];
        this.lowestFrequency = 0;
        this.maxFrequency = maxCacheSize - 1;
        this.maxCacheSize = maxCacheSize;
        this.evictionFactor = evictionFactor;
        initFrequencyList();
    }

    public void put(Key k, Value v) {
        CacheNode<Key, Value> currentNode = cache.get(k);
        if (currentNode == null) {
            if (cache.size() == maxCacheSize) {
                doEviction();
            }
            LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[0];
            currentNode = new CacheNode(k, v, 0);
            nodes.add(currentNode);
            cache.put(k, currentNode);
            lowestFrequency = 0;
        } else {
            currentNode.v = v;
        }
    }

    public Value get(Key k) {
        CacheNode<Key, Value> currentNode = cache.get(k);
        if (currentNode != null) {
            int currentFrequency = currentNode.frequency;
            if (currentFrequency < maxFrequency) {
                int nextFrequency = currentFrequency + 1;
                LinkedHashSet<CacheNode<Key, Value>> currentNodes = frequencyList[currentFrequency];
                LinkedHashSet<CacheNode<Key, Value>> newNodes = frequencyList[nextFrequency];
                moveToNextFrequency(currentNode, nextFrequency, currentNodes, newNodes);
                cache.put(k, currentNode);
                if (lowestFrequency == currentFrequency && currentNodes.isEmpty()) {
                    lowestFrequency = nextFrequency;
                }
            } else {
                // Hybrid with LRU: put most recently accessed ahead of others:
                LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[currentFrequency];
                nodes.remove(currentNode);
                nodes.add(currentNode);
            }
            return currentNode.v;
        } else {
            return null;
        }
    }

    public Value remove(Key k) {
        CacheNode<Key, Value> currentNode = cache.remove(k);
        if (currentNode != null) {
            LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[currentNode.frequency];
            nodes.remove(currentNode);
            if (lowestFrequency == currentNode.frequency) {
                findNextLowestFrequency();
            }
            return currentNode.v;
        } else {
            return null;
        }
    }

    public int frequencyOf(Key k) {
        CacheNode<Key, Value> node = cache.get(k);
        if (node != null) {
            return node.frequency + 1;
        } else {
            return 0;
        }
    }

    public void clear() {
        for (int i = 0; i <= maxFrequency; i++) {
            frequencyList[i].clear();
        }
        cache.clear();
        lowestFrequency = 0;
    }

    public int size() {
        return cache.size();
    }
    
    private void initFrequencyList() {
        for (int i = 0; i <= maxFrequency; i++) {
            frequencyList[i] = new LinkedHashSet<CacheNode<Key, Value>>();
        }
    }

    private void doEviction() {
        int currentlyDeleted = 0;
        float target = maxCacheSize * evictionFactor;
        while (currentlyDeleted < target) {
            LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[lowestFrequency];
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
                    findNextLowestFrequency();
                }
            }
        }
    }

    private void moveToNextFrequency(CacheNode<Key, Value> currentNode, int nextFrequency, LinkedHashSet<CacheNode<Key, Value>> currentNodes, LinkedHashSet<CacheNode<Key, Value>> newNodes) {
        currentNodes.remove(currentNode);
        newNodes.add(currentNode);
        currentNode.frequency = nextFrequency;
    }

    private void findNextLowestFrequency() {
        while (lowestFrequency <= maxFrequency && frequencyList[lowestFrequency].isEmpty()) {
            lowestFrequency++;
        }
        if (lowestFrequency > maxFrequency) {
            lowestFrequency = 0;
        }
    }

    private static class CacheNode<Key, Value> {

        public final Key k;
        public Value v;
        public int frequency;

        public CacheNode(Key k, Value v, int frequency) {
            this.k = k;
            this.v = v;
            this.frequency = frequency;
        }

    }
}
