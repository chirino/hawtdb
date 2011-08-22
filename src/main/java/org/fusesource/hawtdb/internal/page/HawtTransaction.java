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
package org.fusesource.hawtdb.internal.page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.fusesource.hawtdb.api.*;
import org.fusesource.hawtdb.api.PagedAccessor;
import org.fusesource.hawtdb.internal.util.Ranges;
import org.fusesource.hawtdb.util.StringSupport;
import org.fusesource.hawtbuf.Buffer;

import static org.fusesource.hawtdb.internal.page.DeferredUpdate.*;
import static org.fusesource.hawtdb.internal.page.Update.update;

/**
 * Transaction objects are NOT thread safe. Users of this object should
 * guard it from concurrent access.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final class HawtTransaction implements Transaction {

    /**
     * 
     */
    private final HawtTxPageFile parent;

    /**
     * @param concurrentPageFile
     */
    HawtTransaction(HawtTxPageFile concurrentPageFile) {
        parent = concurrentPageFile;
    }

    private ConcurrentHashMap<Integer, Update> updates;
    private ArrayList<Runnable> flushCallbacks;
    private Snapshot snapshot;
    private boolean closed;
    
    private final Allocator txallocator = new Allocator() {
        
        public void free(int pageId, int count) {
            assertOpen();
            // TODO: this is not a very efficient way to handle allocation ranges.
            int end = pageId+count;
            for (int key = pageId; key < end; key++) {
                Update previous = getUpdates().put(key, update().freed(true).note("free "+key) );
                if( previous!=null && previous.allocated() ) {
                    getUpdates().remove(key);
                    HawtTransaction.this.parent.allocator.free(key, 1);
                }
            }
        }
        
        public int alloc(int count) throws OutOfSpaceException {
            assertOpen();
            int pageId = palloc(count);
            // TODO: this is not a very efficient way to handle allocation ranges.
            int end = pageId+count;
            for (int key = pageId; key < end; key++) {
                getUpdates().put(key, update().allocated(true).note("alloc "+key) );
            }
            return pageId;
        }

        public void unfree(int pageId, int count) {
            assertOpen();
            throw new UnsupportedOperationException();
        }
        
        public void clear() throws UnsupportedOperationException {
            assertOpen();
            throw new UnsupportedOperationException();
        }

        public int getLimit() {
            assertOpen();
            return HawtTransaction.this.parent.allocator.getLimit();
        }

        public boolean isAllocated(int page) {
            assertOpen();
            return HawtTransaction.this.parent.allocator.isAllocated(page);
        }

        public void setFreeRanges(Ranges freeList) {
            throw new UnsupportedOperationException();
        }

        public Ranges getFreeRanges() {
            throw new UnsupportedOperationException();
        }

    };


    private void assertOpen() {
        assert !closed : "The transaction had been closed.";
    }

    public void close() {
        assertOpen();
        assert snapshot==null : "The transaction must be committed or rolled back before it is closed.";
        closed = true;
    }

    public void onFlush(Runnable runnable) {
        if( flushCallbacks ==null ) {
            flushCallbacks = new ArrayList<Runnable>(1);
        }
        flushCallbacks.add(runnable);
    }

    public <T> T get(PagedAccessor<T> marshaller, int page) {
        assertOpen();
        // Perhaps the page was updated in the current transaction...
        Update update = updates == null ? null : updates.get(page);
        if( update != null ) {
            if( update.freed() ) {
                throw new PagingException("That page was freed.");
            }
            DeferredUpdate deferred = update.deferredUpdate();
            if( deferred != null ) {
                return deferred.<T>value();
            } else {
                throw new PagingException("That page was updated with the 'put' method.");
            }
        }
        
        // No?  Then ask the snapshot to load the object.
        T rc = snapshot().getTracker().get(marshaller, page);
        if( rc == null ) {
            rc = parent.readCache().cacheLoad(marshaller, page);
        }
        return rc;
    }

    public <T> void put(PagedAccessor<T> marshaller, int page, T value) {
        assertOpen();
        ConcurrentHashMap<Integer, Update> updates = getUpdates();
        Update update = updates.get(page);
        DeferredUpdate deferred = null;
        if (update == null) {
            // This is the first time this transaction updates the page...
            snapshot();
            deferred = deferred();
            updates.put(page, deferred);
        } else {
            // We have updated it before...
            if( update.freed() ) {
                throw new PagingException("You should never try to update a page that has been freed.");
            }
            deferred = update.deferredUpdate();
            if( deferred==null ) {
                deferred = deferred(update);
                updates.put(page, deferred);
            }
        }
        deferred.note("put "+page);
        deferred.put(value, marshaller);
    }

    public <T> void clear(PagedAccessor<T> marshaller, int page) {
        assertOpen();
        ConcurrentHashMap<Integer, Update> updates = getUpdates();
        Update update = updates.get(page);
        
        if( update == null ) {
            updates.put(page, deferred().remove(marshaller).note("clear "+page+" deferred") );
        } else {
            if( !update.put() ) {
                throw new PagingException("You should never try to clear a page that was not put.");
            }

            if( update.allocated() ) {
                updates.put(page, update(update).note("clear "+page+" back to un-deferred"));
            } else {
                // was an update of a previous location....
                updates.put(page, ((DeferredUpdate)update).remove(marshaller).note("clear "+page));
            }
        }
    }
    
    public Allocator allocator() {
        assertOpen();
        return txallocator;
    }
    
    public int alloc() {
        assertOpen();
        return allocator().alloc(1);
    }

    public void free(int page) {
        assertOpen();
        allocator().free(page, 1);
    }

    public void read(int page, Buffer buffer) throws IOPagingException {
        assertOpen();
        // We may need to translate the page due to an update..
        Update update = updates == null ? null : updates.get(page);
        if (update != null && update.shadowed()) {
            // in this transaction..
            page = update.shadow();
        } else {
            // in a committed transaction that has not yet been performed.
            page = snapshot().getTracker().translatePage(page);
        }
        parent.pageFile.read(page, buffer);
    }

    public ByteBuffer slice(SliceType type, int page, int count) throws IOPagingException {
        assertOpen();
        //TODO: wish we could do ranged opps more efficiently.
        
        if( type==SliceType.READ ) {
            Update update = updates == null ? null : updates.get(page);
            if (update != null && update.shadowed() ) {
                page = update.shadow();
            } else {
                page = snapshot().getTracker().translatePage(page);
            }
        } else {
            Update update = getUpdates().get(page);
            if (update == null) {

                // Allocate space of the update redo pages.
                update = update().shadow(palloc(count));
                int end = page+count;
                for (int i = page; i < end; i++) {
                    getUpdates().put(i, update().shadow(i));
                }
                
                if (type==SliceType.READ_WRITE) {
                    // Oh he's going to read it too?? then copy the original to the 
                    // redo pages..
                    int originalPage = snapshot().getTracker().translatePage(page);
                    ByteBuffer slice = parent.pageFile.slice(SliceType.READ, originalPage, count);

                    try {
                        parent.pageFile.write(update.translate(page), slice);
                    } finally { 
                        parent.pageFile.unslice(slice);
                    }
                }
                
                getUpdates().put(page, update);
            }
            
            // translate the page..
            page = update.translate(page);
        }
        return parent.pageFile.slice(type, page, count);
        
    }

    private int palloc(int count) {
        return parent.allocator.alloc(count);
    }

    public void unslice(ByteBuffer buffer) {
        assertOpen();
        parent.pageFile.unslice(buffer);
    }

    public void write(int page, Buffer buffer) throws IOPagingException {
        assertOpen();
        Update update = getUpdates().get(page);
        if (update == null) {
            // We are updating an existing page in the snapshot...
            snapshot();
            update = update().shadow(palloc(1));
            getUpdates().put(page, update);
        }

        if( update.shadowed() ) {
            page = update.shadow();
        }
        parent.pageFile.write(page, buffer);
    }


    public void commit() throws IOPagingException {
        assertOpen();
        boolean failed = true;
        try {
            if (updates!=null) {
                // If the commit is successful it will release our snapshot..
                parent.commit(snapshot, updates, flushCallbacks);
                snapshot = null;
            }
            failed = false;
        } finally {
            // Rollback if the commit fails.
            if (failed) {
                // rollback will release our snapshot..
                rollback();
            }
            updates = null;
            flushCallbacks = null;
            if( snapshot!=null ) {
                snapshot.close();
                snapshot = null;
            }
        }
    }

    public void rollback() throws IOPagingException {
        assertOpen();
        try {
            if (updates!=null) {
                for (Map.Entry<Integer,Update> entry : updates.entrySet()) {
                    Integer page = entry.getKey();
                    Update update = entry.getValue();
                    if( !update.freed() ) {
                        allocator().free(update.translate(page), 1);
                    }
                }
            }
        } finally {
            if( snapshot!=null ) {
                snapshot.close();
                snapshot = null;
            }
            updates = null;
            flushCallbacks = null;
        }
    }

    public Snapshot snapshot() {
        if (snapshot == null) {
            snapshot = parent.openSnapshot();
        }
        return snapshot;
    }

    public boolean isReadOnly() {
        assertOpen();
        return updates == null;
    }

    private ConcurrentHashMap<Integer, Update> getUpdates() {
        if (updates == null) {
            updates = new ConcurrentHashMap<Integer, Update>();
        }
        return updates;
    }

    public int getPageSize() {
        assertOpen();
        return parent.pageFile.getPageSize();
    }

    public String toString() { 
        int updatesSize = updates==null ? 0 : updates.size();
        return "{ \n" +
        	   "  snapshot: "+this.snapshot+", \n"+
        	   "  updates: "+updatesSize+", \n" +
        	   "  parent: "+StringSupport.indent(parent.toString(), 2)+"\n" +
        	   "}";
    }

    public int pages(int length) {
        assertOpen();
        return parent.pageFile.pages(length);
    }

    public void flush() {
        assertOpen();
        parent.flush();
    }

}
