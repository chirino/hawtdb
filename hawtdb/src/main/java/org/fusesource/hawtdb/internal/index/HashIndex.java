/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.hawtdb.internal.index;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtdb.api.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import static org.fusesource.hawtdb.internal.index.Logging.debug;

/**
 * Hash Index implementation.  The hash buckets store entries in a b+tree.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class HashIndex<Key,Value> implements Index<Key,Value> {
    
    private final BTreeIndexFactory<Key, Value> BIN_FACTORY = new BTreeIndexFactory<Key, Value>();
    
    private final Paged paged;
    private final int page;
    private final int maximumBucketCapacity;
    private final int minimumBucketCapacity;
    private final boolean fixedCapacity;
    private final int loadFactor;
    private final int initialBucketCapacity;
    private final boolean deferredEncoding;

    private Buckets<Key,Value> buckets;

    public HashIndex(Paged paged, int page, HashIndexFactory<Key,Value> factory) {
        this.paged = paged;
        this.page = page;
        this.maximumBucketCapacity = factory.getMaximumBucketCapacity();
        this.minimumBucketCapacity = factory.getMinimumBucketCapacity();
        this.loadFactor = factory.getLoadFactor();
        this.deferredEncoding = factory.isDeferredEncoding();
        this.initialBucketCapacity = factory.getBucketCapacity();
        this.BIN_FACTORY.setKeyCodec(factory.getKeyCodec());
        this.BIN_FACTORY.setValueCodec(factory.getValueCodec());
        this.BIN_FACTORY.setDeferredEncoding(this.deferredEncoding);
        this.fixedCapacity = this.minimumBucketCapacity==this.maximumBucketCapacity && this.maximumBucketCapacity==this.initialBucketCapacity;
    }

    public HashIndex<Key, Value> create() {
        buckets = new Buckets<Key, Value>();
        buckets.create(this, initialBucketCapacity);
        storeBuckets();
        return this;
    }

    public HashIndex<Key, Value> open() {
        loadBuckets();
        return this;
    }


    public Value get(Key key) {
        return buckets.bucket(this, key).get(key);
    }
    
    public boolean containsKey(Key key) {
        return buckets.bucket(this, key).containsKey(key);
    }
    
    public Value put(Key key, Value value) {
        
        Index<Key, Value> bucket = buckets.bucket(this, key);
        
        if( fixedCapacity ) {
            return bucket.put(key,value);
        }
        
        boolean wasEmpty = bucket.isEmpty();
        Value put = bucket.put(key,value);

        if (wasEmpty) {
            buckets.active++;
            storeBuckets();
        }
        
        if (buckets.active >= buckets.increaseThreshold) {
            int capacity = Math.min(this.maximumBucketCapacity, buckets.capacity * 4);
            if (buckets.capacity != capacity) {
                this.changeCapacity(capacity);
            }
        }
        return put;
    }

    public Value putIfAbsent(Key key, Value value) {

        Index<Key, Value> bucket = buckets.bucket(this, key);

        if( fixedCapacity ) {
            return bucket.putIfAbsent(key,value);
        }

        boolean wasEmpty = bucket.isEmpty();
        Value put = bucket.putIfAbsent(key,value);

        if (wasEmpty) {
            buckets.active++;
            storeBuckets();
        }

        if (buckets.active >= buckets.increaseThreshold) {
            int capacity = Math.min(this.maximumBucketCapacity, buckets.capacity * 4);
            if (buckets.capacity != capacity) {
                this.changeCapacity(capacity);
            }
        }
        return put;
    }
    
    public Value remove(Key key) {
        Index<Key, Value> bucket = buckets.bucket(this, key);
        
        if( fixedCapacity ) {
            return bucket.remove(key);
        }
        
        boolean wasEmpty = bucket.isEmpty();
        Value rc = bucket.remove(key);
        boolean isEmpty = bucket.isEmpty();
        
        if (!wasEmpty && isEmpty) {
            buckets.active--;
            storeBuckets();
        }

        if (buckets.active <= buckets.decreaseThreshold) {
            int capacity = Math.max(minimumBucketCapacity, buckets.capacity / 2);
            if (buckets.capacity != capacity) {
                changeCapacity(capacity);
            }
        }
        return rc;
    }

    public void clear() {
        buckets.clear(this);
        if (buckets.capacity!=initialBucketCapacity) {
            changeCapacity(initialBucketCapacity);
        }
    }

    public int size() {
        int rc=0;
        for (int i = 0; i < buckets.capacity; i++) {
            rc += buckets.bucket(this, i).size();
        }
        return rc;
    }
    
    public boolean isEmpty() {
        return buckets.active==0;
    }    
    
    public void destroy() {
        buckets.destroy(this);
        buckets = null;
        paged.free(page);
    }

    public int getIndexLocation() {
        return page;
    }

    // /////////////////////////////////////////////////////////////////
    // Helper methods Methods
    // /////////////////////////////////////////////////////////////////
    private void changeCapacity(final int capacity) {
        debug("Resizing to: %d", capacity);
        
        Buckets<Key, Value> next = new Buckets<Key, Value>();
        next.create(this, capacity);

        // Copy the data from the old buckets to the new buckets.
        for (int i = 0; i < buckets.capacity; i++) {
            SortedIndex<Key, Value> bin = buckets.bucket(this, i);
            HashSet<Integer> activeBuckets = new HashSet<Integer>();
            for (Map.Entry<Key, Value> entry : bin) {
                Key key = entry.getKey();
                Value value = entry.getValue();
                Index<Key, Value> bucket = next.bucket(this, key);
                bucket.put(key, value);
                if( activeBuckets.add(bucket.getIndexLocation()) ) {
                    next.active++;
                }
            }
        }
        
        buckets.destroy(this);
        buckets = next;
        storeBuckets();
        
        debug("Resizing done.");
    }

    public String toString() {
        return "{ page: "+page+", buckets: "+buckets+" }";
    }
    
    private void storeBuckets() {
        if( deferredEncoding ) {
            paged.put(BUCKET_PAGED_ACCESSOR, page, buckets);
        } else {
            BUCKET_PAGED_ACCESSOR.store(paged, page, buckets);
        }
    }

    private void loadBuckets() {
        if( deferredEncoding ) {
            buckets = paged.get(BUCKET_PAGED_ACCESSOR, page);
        } else {
            buckets = BUCKET_PAGED_ACCESSOR.load(paged, page);
        }
    }
    
    // /////////////////////////////////////////////////////////////////
    // Helper classes
    // /////////////////////////////////////////////////////////////////

    /** 
     * This is the data stored in the index header.  It knows where
     * the hash buckets are stored at an keeps usage statistics about
     * those buckets. 
     */
    static private class Buckets<Key,Value> {

        int active;
        int capacity;
        int[] bucketsIndex;

        int increaseThreshold;
        int decreaseThreshold;

        final ConcurrentMap<Integer, SortedIndex<Key, Value>> buckets = new ConcurrentHashMap<Integer, SortedIndex<Key, Value>>();
        
        private void calcThresholds(HashIndex<Key,Value> index) {
            increaseThreshold = (capacity * index.loadFactor)/100;
            decreaseThreshold = (capacity * index.loadFactor * index.loadFactor ) / 20000;
        }

        void create(HashIndex<Key,Value> index, int capacity) {
            this.active = 0;
            this.capacity = capacity;
            this.bucketsIndex = new int[capacity];
            for (int i = 0; i < capacity; i++) {
                this.bucketsIndex[i] = index.BIN_FACTORY.create(index.paged).getIndexLocation();
            }
            calcThresholds(index);
        }
        
        public void destroy(HashIndex<Key,Value> index) {
            clear(index);
            for (int i = 0; i < capacity; i++) {
                index.paged.allocator().free(bucketsIndex[i], 1);
            }
        }
        
        public void clear(HashIndex<Key,Value> index) {
            this.buckets.clear();
            for (int i = 0; i < index.buckets.capacity; i++) {
                index.buckets.bucket(index, i).clear();
            }
            index.buckets.active = 0;
            index.buckets.calcThresholds(index);
        }
        
        SortedIndex<Key,Value> bucket(HashIndex<Key,Value> index, int bucket) {
            return getOrOpen(index, bucketsIndex[bucket]);
        }

        SortedIndex<Key,Value> bucket(HashIndex<Key,Value> index, Key key) {
            int i = index(key);
            return getOrOpen(index, bucketsIndex[i]);
        }

        int index(Key x) {
            return Math.abs(x.hashCode()%capacity);
        }
        
        private SortedIndex<Key,Value> getOrOpen(HashIndex<Key,Value> hash, int location) {
            SortedIndex<Key,Value> result = buckets.get(location);
            if (result == null) {
                SortedIndex<Key,Value> bin = hash.BIN_FACTORY.open(hash.paged, location);
                result = buckets.putIfAbsent(location, bin);
                if (result == null) {
                    result = bin;
                }
            }
            return result;
        }
        
        @Override
        public String toString() {
            return "{ capacity: "+capacity+", active: "+active+", increase threshold: "+increaseThreshold+", decrease threshold: "+decreaseThreshold+" }";
        }
        
    }

    public static final Buffer MAGIC = new Buffer(new byte[] {'h', 'a', 's', 'h'});
    public static final int HEADER_SIZE = MAGIC.length + 8; // capacity, active

    private final PagedAccessor<Buckets<Key, Value>> BUCKET_PAGED_ACCESSOR = new AbstractStreamPagedAccessor<Buckets<Key, Value>>() {

        @Override
        protected void encode(Paged paged, DataOutputStream os, Buckets<Key, Value> data) throws IOException {
            os.write(MAGIC.data, MAGIC.offset, MAGIC.length);

            os.writeInt(buckets.active);
            os.writeInt(buckets.capacity);

            for (int i =0; i < buckets.capacity; i++) {
                os.writeInt(buckets.bucketsIndex[i]);
            }
        }

        @Override
        protected Buckets<Key, Value> decode(Paged paged, DataInputStream is) throws IOException {
            Buckets<Key, Value> buckets = new Buckets<Key, Value>();

            Buffer magic = new Buffer(MAGIC.length);
            is.readFully(magic.data, magic.offset, magic.length);
            if (!magic.equals(MAGIC)) {
                throw new IOException("Not a hash page");
            }

            buckets.active = is.readInt();
            buckets.capacity = is.readInt();

            buckets.bucketsIndex = new int[buckets.capacity];
            for (int i =0; i < buckets.capacity; i++) {
                buckets.bucketsIndex[i] = is.readInt();
            }

            return buckets;
        }
        
    };

}
