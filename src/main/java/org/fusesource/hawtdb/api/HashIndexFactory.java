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
package org.fusesource.hawtdb.api;

import org.fusesource.hawtdb.internal.index.HashIndex;
import org.fusesource.hawtdb.util.marshaller.Marshaller;
import org.fusesource.hawtdb.util.marshaller.ObjectMarshaller;

/**
 * <p>
 * Uses to create Hash based storage of key/values.  The hash index
 * consists of contiguous array of pages allocated on the page file.
 * Each page is considered a bucket.  keys get hashed to a bucket 
 * indexes and the key and value are stored in the bucket.  Each 
 * bucket is actually a BTree root and can therefore store multiple 
 * keys and values and overflow to additional pages if needed.    
 * </p>
 * <p>
 * Once the percentage of hash buckets used passes the configured
 * load factor, the hash index will "resize" to use a larger number
 * of buckets to keep the number items per bucket low which increases
 * access time of the items as there are fewer page access required.
 * </p>
 * <p>
 * Unlike BTree indexes, Hash indexes are not kept in key sorted order.
 * </p>
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class HashIndexFactory<Key, Value> implements IndexFactory<Key, Value> {
    
    public static final String PROPERTY_PREFIX = HashIndex.class.getName()+".";
    public static final int DEFAULT_BUCKET_CAPACITY = Integer.parseInt(System.getProperty(PROPERTY_PREFIX+"DEFAULT_BUCKET_CAPACITY", "1024"));
    public static final int DEFAULT_MAXIMUM_BUCKET_CAPACITY = Integer.parseInt(System.getProperty(PROPERTY_PREFIX+"DEFAULT_MAXIMUM_BUCKET_CAPACITY", "16384"));
    public static final int DEFAULT_MINIMUM_BUCKET_CAPACITY = Integer.parseInt(System.getProperty(PROPERTY_PREFIX+"DEFAULT_MINIMUM_BUCKET_CAPACITY", "16"));
    public static final int DEFAULT_LOAD_FACTOR = Integer.parseInt(System.getProperty(PROPERTY_PREFIX+"DEFAULT_LOAD_FACTOR", "75"));
    
    private Marshaller<Key> keyMarshaller = new ObjectMarshaller<Key>();
    private Marshaller<Value> valueMarshaller = new ObjectMarshaller<Value>();
    private int initialBucketCapacity = DEFAULT_BUCKET_CAPACITY;
    private int maximumBucketCapacity = DEFAULT_MAXIMUM_BUCKET_CAPACITY;
    private int minimumBucketCapacity = DEFAULT_MINIMUM_BUCKET_CAPACITY;
    private int loadFactor = DEFAULT_LOAD_FACTOR;
    private boolean deferredEncoding=true;

    /**
     * Loads an existing hash index from the paged object at the given page location.
     */
    public Index<Key, Value> open(Paged paged, int page) {
        return createInstance(paged, page).open();
    }

    /**
     * Creates a new hash index on the Paged object at the given page location.
     */
    public Index<Key, Value> create(Paged paged, int page) {
        return createInstance(paged, page).create();
    }

    private HashIndex<Key, Value> createInstance(Paged paged, int page) {
        return new HashIndex<Key, Value>(paged, page, this);
    }

    /**
     * Defaults to an {@link ObjectMarshaller} if not explicitly set.
     * 
     * @return the marshaller used for keys.
     */
    public Marshaller<Key> getKeyMarshaller() {
        return keyMarshaller;
    }

    /**
     * Allows you to configure custom marshalling logic to encode the index keys.
     * 
     * @param marshaller the marshaller used for keys.
     */
    public void setKeyMarshaller(Marshaller<Key> marshaller) {
        this.keyMarshaller = marshaller;
    }

    /**
     * Defaults to an {@link ObjectMarshaller} if not explicitly set.
     *  
     * @return the marshaller used for values.
     */
    public Marshaller<Value> getValueMarshaller() {
        return valueMarshaller;
    }

    /**
     * Allows you to configure custom marshalling logic to encode the index values.
     * 
     * @param marshaller the marshaller used for values
     */
    public void setValueMarshaller(Marshaller<Value> marshaller) {
        this.valueMarshaller = marshaller;
    }

    /**
     * @return the maximum bucket capacity
     */
    public int getMaximumBucketCapacity() {
        return maximumBucketCapacity;
    }

    /**
     * Sets the maximum bucket capacity.
     * 
     * @param value the new capacity
     */
    public void setMaximumBucketCapacity(int value) {
        this.maximumBucketCapacity = value;
    }

    /**
     * 
     * @return the minimum bucket capacity
     */
    public int getMinimumBucketCapacity() {
        return minimumBucketCapacity;
    }

    /**
     * Sets the minimum bucket capacity.
     * 
     * @param value the new capacity
     */
    public void setMinimumBucketCapacity(int value) {
        this.minimumBucketCapacity = value;
    }

    /**
     * @return the index load factor
     */
    public int getLoadFactor() {
        return loadFactor;
    }

    /**
     * Sets the index load factor.  When this load factor percentage
     * of used buckets is execeeded, the index will resize to increase the bucket capacity. 
     * @param loadFactor
     */
    public void setLoadFactor(int loadFactor) {
        this.loadFactor = loadFactor;
    }

    /**
     * @return the initial bucket capacity
     */
    public int getBucketCapacity() {
        return initialBucketCapacity;
    }

    /**
     * sets the initial bucket capacity. 
     * @param binCapacity
     */
    public void setBucketCapacity(int binCapacity) {
        this.initialBucketCapacity = binCapacity;
    }

    /**
     * Convenience method which sets the maximum, minimum and initial bucket capacity to be the specified value.
     * @param value
     */
    public void setFixedCapacity(int value) {
        this.minimumBucketCapacity = this.maximumBucketCapacity = this.initialBucketCapacity = value;
    }

    /**
     * 
     * @return true if deferred encoding enabled
     */
    public boolean isDeferredEncoding() {
        return deferredEncoding;
    }

    /**
     * <p>
     * When deferred encoding is enabled, the index avoids encoding keys and values
     * for as long as possible so take advantage of collapsing multiple updates of the 
     * same key/value into a single update operation and single encoding operation.
     * </p><p>
     * Using this feature requires the keys and values to be immutable objects since 
     * unexpected errors would occur if they are changed after they have been handed
     * to to the index for storage. 
     * </p>
     * @param enable should deferred encoding be enabled.
     */
    public void setDeferredEncoding(boolean enable) {
        this.deferredEncoding = enable;
    }
    
}