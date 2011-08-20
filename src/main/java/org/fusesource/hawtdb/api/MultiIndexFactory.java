package org.fusesource.hawtdb.api;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Special purpose index factory providing APIs to create and open several indexes at a given {@link Paged} object:
 * each index is identified by a unique name, which must be used to refer to the index itself when opening or creating it.
 * <br>
 * This index factory is very useful when having to manage several correlated indexes, whose updates must be atomically executed
 * in the same transaction.
 * 
 * @author Sergio Bossa
 */
public class MultiIndexFactory {

    private final static BTreeIndexFactory<String, Integer> INTERNAL_INDEX_FACTORY = new BTreeIndexFactory<String, Integer>();
    //
    private final SortedIndex<String, Integer> multiIndex;

    public MultiIndexFactory(Paged paged) {
        multiIndex = INTERNAL_INDEX_FACTORY.openOrCreate(paged);
    }

    /**
     * Create a new named index.
     * 
     * @param <Key> The index key type.
     * @param <Value> The index value type.
     * @param paged The paged object used to store the index.
     * @param indexName The unique index name.
     * @param indexFactory The index factory used to actually create the index.
     * @return The newly created index.
     */
    public <Key, Value> Index<Key, Value> create(Paged paged, String indexName, IndexFactory<Key, Value> indexFactory) {
        if (!multiIndex.containsKey(indexName)) {
            Index<Key, Value> result = indexFactory.create(paged);
            multiIndex.put(indexName, result.getIndexLocation());
            return result;
        } else {
            throw new IllegalStateException("Index " + indexName + " already exists!");
        }
    }

    /**
     * Open named index.
     * 
     * @param <Key> The index key type.
     * @param <Value> The index value type.
     * @param paged The paged object used to load the index.
     * @param indexName The unique index name.
     * @param indexFactory The index factory used to actually open the index.
     * @return The opened index.
     */
    public <Key, Value> Index<Key, Value> open(Paged paged, String indexName, IndexFactory<Key, Value> indexFactory) {
        Integer location = multiIndex.get(indexName);
        if (location != null) {
            Index<Key, Value> result = indexFactory.open(paged, location);
            return result;
        } else {
            throw new IllegalStateException("Index " + indexName + " doesn't exist!");
        }
    }

    /**
     * Open or create the named index.
     * 
     * @param <Key> The index key type.
     * @param <Value> The index value type.
     * @param paged The paged object used to load/store the index.
     * @param indexName The unique index name.
     * @param indexFactory The index factory used to actually open or create the index.
     * @return The index.
     */
    public <Key, Value> Index<Key, Value> openOrCreate(Paged paged, String indexName, IndexFactory<Key, Value> indexFactory) {
        Integer location = multiIndex.get(indexName);
        Index<Key, Value> result = null;
        if (location != null) {
            result = indexFactory.open(paged, location);
        } else {
            result = indexFactory.create(paged);
            multiIndex.put(indexName, result.getIndexLocation());
        }
        return result;
    }

    /**
     * List the names of all indexes stored in the paged object.
     * 
     * @return The indexes names.
     */
    public Set<String> indexes() {
        Set<String> result = new HashSet<String>();
        for (Map.Entry<String, Integer> entry : multiIndex) {
            result.add(entry.getKey());
        }
        return result;
    }

}
