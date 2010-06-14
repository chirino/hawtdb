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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.fusesource.hawtdb.api.*;
import org.fusesource.hawtdb.api.PagedAccessor;
import org.fusesource.hawtdb.api.Paged.SliceType;
import org.fusesource.hawtdb.internal.io.MemoryMappedFile;
import org.fusesource.hawtdb.internal.util.Ranges;
import org.fusesource.hawtdb.util.LRUCache;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.fusesource.hawtdb.util.list.LinkedNodeList;

/**
 * Provides concurrent page file access via Multiversion concurrency control
 * (MVCC).
 * 
 * Once a transaction begins working against the data, it acquires a snapshot of
 * all the data in the page file. This snapshot is used to provides the
 * transaction consistent view of the data in spite of it being concurrently
 * modified by other transactions.
 * 
 * When a transaction does a page update, the update is stored in a temporary
 * page location. Subsequent reads of the original page will result in page read
 * of the temporary page. If the transaction rolls back, the temporary pages are
 * freed. If the transaction commits, the page updates are assigned the next
 * snapshot version number and the update gets queued so that it can be applied
 * atomically at a later time.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public final class HawtTxPageFile implements TxPageFile {

    private static final Log LOG = LogFactory.getLog(HawtTxPageFile.class);
    public static final int FILE_HEADER_SIZE = 1024 * 4;
    public static final  byte[] MAGIC = magic(); 

    private static byte[] magic() {
        try {
            byte rc[] = new byte[32];
            byte[] tmp = "HawtDB:1.0\n".getBytes("UTF-8");
            System.arraycopy(tmp, 0, rc, 0, tmp.length);
            return rc;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * The first 4K of the file is used to hold 2 copies of the header.
     * Each copy is 2K big.  The header is checksummed so that corruption
     * can be detected. 
     */
    static private class Header {
        
        /** Identifies the file format */
        public byte[] magic = new byte[32];
        /** The oldest applied commit revision */
        public long base_revision;
        /** The size of each page in the page file */
        public int page_size;
        /** The page location of the free page list */
        public int free_list_page;

        /** Where it is safe to resume recovery... Will be
         *  -1 if no recovery is needed. */
        public int pessimistic_recovery_page;

        /** We try to recover from this point.. but it may fail since it's
         *  writes have not been synced to disk. */
        public int optimistic_recovery_page;

        public String toString() { 
            return "{ base_revision: "+this.base_revision+
            ", page_size: "+page_size+", free_list_page: "+free_list_page+
            ", pessimistic_recovery_page: "+ pessimistic_recovery_page +
            ", optimistic_recovery_page: "+ optimistic_recovery_page +
            " }";
        }
        
        private final DataByteArrayOutputStream os = new DataByteArrayOutputStream(FILE_HEADER_SIZE);
        
        Buffer encode() {
            try {
                os.reset();
                os.write(magic);
                os.writeLong(base_revision);
                os.writeInt(page_size);
                os.writeInt(free_list_page);
                os.writeInt(pessimistic_recovery_page);
                os.writeInt(optimistic_recovery_page);

                int length = os.position();
                byte[] data = os.getData();
                
                CRC32 checksum = new CRC32();
                checksum.update(data, 0, length);
                
                os.position((FILE_HEADER_SIZE/2)-8);
                os.writeLong(checksum.getValue());
                System.arraycopy(data, 0, data, FILE_HEADER_SIZE/2, length);
                os.position(FILE_HEADER_SIZE/2-8);
                os.writeLong(checksum.getValue());

                return os.toBuffer();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
        void decode(Buffer buffer) throws PagingException {
            DataByteArrayInputStream is = new DataByteArrayInputStream(buffer);
            int length = readFields(is);
            is.setPos((FILE_HEADER_SIZE/2)-8);
            long expectedChecksum = is.readLong();
            CRC32 checksum = new CRC32();
            checksum.update(buffer.data, 0, length);
            if( checksum.getValue() != expectedChecksum ) {
                // Try the 2nd copy..
                is.setPos(FILE_HEADER_SIZE/2);
                length = readFields(is);
                is.setPos(FILE_HEADER_SIZE-8);
                expectedChecksum = is.readLong();
                checksum = new CRC32();
                checksum.update(buffer.data, 0, length);
                if( checksum.getValue() != expectedChecksum ) {
                    throw new PagingException("file header corruption detected.");
                }                    
            }
        }

        private int readFields(DataByteArrayInputStream is) {
            is.readFully(magic);
            base_revision = is.readLong();
            page_size = is.readInt();
            free_list_page = is.readInt();
            pessimistic_recovery_page = is.readInt();
            optimistic_recovery_page = is.readInt();
            int length = is.getPos();
            return length;
        }
        
    }
    /** The header structure of the file */
    private final Header header = new Header();
    private final LinkedNodeList<Batch> batches = new LinkedNodeList<Batch>();

    private final MemoryMappedFile file;
    final Allocator allocator;
    final HawtPageFile pageFile;
    private static final int updateBatchSize = 1024;
    private final boolean synch;
    private int lastBatchPage = -1;
    
    //
    // The following batch objects point to linked nodes in the previous batch list.  
    // They are used to track/designate the state of the batch object.
    //
    
    /** The current batch that is currently being assembled. */
    Batch openBatch;
    /** The batches that are being stored... These might be be recoverable. */
    Batch storingBatches;
    /** The stored batches. */
    Batch storedBatches;
    /** The performed batches.  Page updates have been copied from the redo pages to the original page locations. */
    Batch performedBatches;
    
    /** A read cache used to speed up access to frequently used pages */
    ReadCache readCache;

    //
    // Profilers like yourkit just tell which mutex class was locked.. so create a different class for each mutex
    // so we can more easily tell which mutex was locked.
    //
    private static class HOUSE_KEEPING_MUTEX { public String toString() { return "HOUSE_KEEPING_MUTEX"; }}
    private static class TRANSACTION_MUTEX { public String toString() { return "TRANSACTION_MUTEX"; }}

    /** 
     * Mutex for data structures which are used during house keeping tasks like batch
     * management. Once acquired, you can also acquire the TRANSACTION_MUTEX 
     */
    private final HOUSE_KEEPING_MUTEX HOUSE_KEEPING_MUTEX = new HOUSE_KEEPING_MUTEX();

    /** 
     * Mutex for data structures which transaction threads access. Never attempt to 
     * acquire the HOUSE_KEEPING_MUTEX once this mutex is acquired.  
     */
    final TRANSACTION_MUTEX TRANSACTION_MUTEX = new TRANSACTION_MUTEX();
    
    /**
     * This is the free page list at the base revision.  It does not 
     * track allocations in transactions or committed updates.  Only 
     * when the updates are performed will this list be updated.
     * 
     * The main purpose of this list is to initialize the free list 
     * on recovery.
     * 
     * This does not track the space associated with batch lists 
     * and free lists.  On recovery that space is discovered and 
     * tracked in the page file allocator.
     */
    private Ranges storedFreeList = new Ranges();
    private final ExecutorService worker;
    
    public HawtTxPageFile(TxPageFileFactory factory, HawtPageFile pageFile) {
        this.pageFile = pageFile;
        this.synch = factory.isSync();
        this.file = pageFile.getFile();
        this.allocator = pageFile.allocator();

        readCache = new ReadCache(factory.getCacheSize());
        
        if( factory.isUseWorkerThread() ) {
            worker = Executors.newSingleThreadExecutor(new ThreadFactory() {
                public Thread newThread(Runnable r) {
                    Thread rc = new Thread(r);
                    rc.setName("HawtDB Worker");
                    rc.setDaemon(true);
                    return rc;
                }
            });
        } else {
            worker = null;
        }
    }
    
    @Override
    public String toString() {
        return "{\n" +
    		"  allocator: "+allocator+ ",\n"+
    		"  synch: "+synch+ ",\n"+
    		"  read cache size: "+readCache.map.size()+ ",\n"+
    		"  base revision free pages: "+storedFreeList + ",\n"+
    		"  batches: {\n"+ 
    		"    performed: "+toString(performedBatches, storedBatches) + ",\n"+ 
    		"    stored: "+toString(storedBatches, storingBatches) + ",\n"+
    		"    storing: "+toString(storingBatches, openBatch)+ ",\n"+
    		"    open: "+toString(openBatch, null)+ ",\n"+
    		"  }"+ "\n"+
    		"}";
    }

    /** 
     * @param from
     * @param to
     * @return string representation of the batch items from the specified batch up to (exclusive) the specified batch.
     */
    private String toString(Batch from, Batch to) {
        StringBuilder rc = new StringBuilder();
        rc.append("[ ");
        Batch t = from;
        while( t!=null && t!=to ) {
            if( t!=from ) {
                rc.append(", ");
            }
            rc.append(t);
            t = t.getNext();
        }
        rc.append(" ]");
        return rc.toString();
    }

    /* (non-Javadoc)
     * @see org.fusesource.hawtdb.internal.page.TransactionalPageFile#tx()
     */
    public Transaction tx() {
        return new HawtTransaction(this);
    }

    /**
     * Attempts to commit a set of page updates.
     * 
     * @param snapshot
     * @param pageUpdates
     */
    void commit(Snapshot snapshot, ConcurrentHashMap<Integer, Update> pageUpdates) {
        
        boolean fullBatch=false;
        Commit commit=null;
        synchronized (TRANSACTION_MUTEX) {
            
            // we need to figure out the revision id of the this commit...
            long rev;
            if( snapshot!=null ) {
                
                // Lets check for an OptimisticUpdateException
                // verify that the new commit's updates don't conflict with a commit that occurred
                // subsequent to the snapshot that this commit started operating on.
                
                // Note: every deferred update has an entry in the pageUpdates, so no need to 
                // check to see if that map also conflicts.
                rev = snapshot.getTracker().commitCheck(pageUpdates);
                snapshot.close();
            } else {
                rev = openBatch.head;
            }
            rev++;

            commit = openBatch.commits.getTail();

            if( commit!=null && commit.snapshotTracker==null ) {
                // just merge /w the previous commit if it does not have an open snapshot.
                // TODO: we are inside the TRANSACTION_MUTEX ... and this seems CPU intensive..
                // but it's better than always creating more commit entries.. as that slows down
                // page look up (the have to iterate through all the commits).
                commit.merge(pageFile.allocator(), rev, pageUpdates);
            } else {
                commit = new Commit(rev, pageUpdates);
                openBatch.commits.addLast(commit);
            }
            
            if( openBatch.base == -1 ) {
                openBatch.base = rev;
            }
            openBatch.head = rev;

            
            if( openBatch.pageCount() > updateBatchSize ) {
                fullBatch = true;
            }
        }
        
        if( fullBatch ) {
            synchronized (HOUSE_KEEPING_MUTEX) {
                storeBatches(false);
            }
            if( worker!=null ) {
                worker.execute(new Runnable() {
                    public void run() {
                        flushBatch();
                    }
                });
            } else {
                flushBatch();
            }
        }
    }

    private void flushBatch() {
        synchronized (HOUSE_KEEPING_MUTEX) {
            // TODO: do the following actions async.
            syncBatches();
            performBatches();
        }
    }
    
    /**
     * Used to initialize a new file or to clear out the 
     * contents of an existing file.
     */
    public void reset() {
        synchronized (HOUSE_KEEPING_MUTEX) {
            batches.clear();
            performedBatches = storedBatches = storingBatches = openBatch = new Batch(-1);
            batches.addFirst(openBatch);
            
            lastBatchPage = -1;
            readCache.clear();
            
            allocator.clear(); 
            storedFreeList.clear();
            storedFreeList.add(0, allocator.getLimit());
    
            // Initialize the file header..
            System.arraycopy(MAGIC, 0, header.magic, 0, MAGIC.length);
            header.base_revision = -1;
            header.free_list_page = -1;
            header.page_size = pageFile.getPageSize();
            header.pessimistic_recovery_page = -1;
            header.optimistic_recovery_page = -1;
            storeHeader();
        }
    }    
    /**
     * Loads an existing file and replays the batch
     * logs to put it in a consistent state.
     */
    public void recover() {
        synchronized (HOUSE_KEEPING_MUTEX) {

            batches.clear();
            performedBatches = storedBatches = storingBatches = openBatch = new Batch(-1);
            batches.addFirst(openBatch);
            lastBatchPage = -1;
            readCache.clear();
    
            Buffer buffer = new Buffer(FILE_HEADER_SIZE);
            file.read(0, buffer);
            header.decode(buffer);

            if( !Arrays.equals(MAGIC, header.magic) ) {
                throw new PagingException("The file header is not of the expected type.");
            }

            LOG.debug("recovery started.  header: "+header);

            // Initialize the free page list.
            if( header.free_list_page >= 0 ) {
                storedFreeList = loadObject(header.free_list_page);
                allocator.copy(storedFreeList);
                Extent.unfree(pageFile, header.free_list_page);
            } else {
                allocator.clear(); 
                storedFreeList.add(0, allocator.getLimit());
            }

            int pageId = header.pessimistic_recovery_page;
            if( header.optimistic_recovery_page >= 0 ) {
                pageId = header.optimistic_recovery_page;
            }

            LinkedList<Batch> loaded = new LinkedList<Batch>();

            boolean consistencyCheckNeeded = true;
            while( pageId >= 0  ) {

                LOG.trace("loading batch at: "+pageId);
                Batch batch = null;

                if( pageId == header.pessimistic_recovery_page) {
                    consistencyCheckNeeded = false;
                }

                if( consistencyCheckNeeded ) {
                    // write could be corrupted.. lets be careful
                    try {
                        batch = loadObject(pageId);
                    } catch(Exception e) {
                        LOG.trace("incomplete batch at: "+pageId);
                        // clear out any previously loaded batchs.. and
                        // resume from the pessimistic location.
                        loaded.clear();
                        pageId = header.pessimistic_recovery_page;
                        continue;
                    }
                } else {
                    // it should load fine..
                    batch = loadObject(pageId);
                }

                batch.page = pageId;
                batch.recovered = true;
                loaded.add(batch);

                LOG.trace("loaded batch: "+batch);

                // is this the last batch we need to load?
                if( header.base_revision+1 == batch.base ) {
                    break;
                }

                pageId=batch.previous;
            }

            if( loaded.isEmpty() ) {
                LOG.trace("no batches need to be recovered.");
            } else {

                // link up the batch objects...
                for (Batch batch : loaded) {

                    // makes sure the batch pages are not in the free list.
                    Extent.unfree(pageFile, batch.page);

                    if( openBatch.head == -1 ) {
                        openBatch.head = batch.head;
                    }

                    // add first since we are loading batch objects oldest to youngest
                    // but want to put them in the list youngest to oldest.
                    batches.addFirst(batch);
                    performedBatches = storedBatches = batch;
                }

                // Perform the updates..
                performBatches();
                syncBatches();
            }
        }
    }

    /* (non-Javadoc)
     * @see org.fusesource.hawtdb.internal.page.TransactionalPageFile#flush()
     */
    public void flush() {
        synchronized (HOUSE_KEEPING_MUTEX) {
            storeBatches(true);
            syncBatches();
        }
    }

    public void flush(final Runnable onComplete) {
        if( worker!=null ) {
            worker.execute(new Runnable() {
                public void run() {
                    flush();
                    onComplete.run();
                }
            });
        } else {
            flush();
            onComplete.run();
        }

    }
    
    // /////////////////////////////////////////////////////////////////
    //
    // Methods which transition bathes through their life cycle states:
    // open -> storing -> stored -> performing -> performed -> released
    //
    //   state: open - you can add additional commits to the batch
    //
    //   on: batch size limit reached
    //   action: write the batch to disk
    //           update optimistic_recovery_page
    //
    //   state: storing - batch was written to disk, but not synced.. batch may be lost on failure.
    //
    //      on: disk sync
    //      action: update pessimistic_recovery_page
    //
    //   state: stored - we know know the batch can be recovered.  Updates will not be lost once we hit this state.
    //
    //     on: original pages drained of open snapshots
    //     action: copy shadow pages to original pages
    //
    //   state: performing - original pages are being updated.  Updates might be partially applied.
    //
    //      on: disk sync
    //
    //   state performed: original pages no updated.
    //
    //      action: the batch becomes the base revision, new snapshot can refer to the original page locations.
    //
    //      on: batch drained of open snapshots
    //
    //   state: released - The batch is no longer being used.
    //
    //      action: free the batch shadow pages
    //
    //
    // /////////////////////////////////////////////////////////////////
    
    /**
     * Attempts to perform a batch state change: open -> storing
     */
    private void storeBatches(boolean force) {
        Batch batch;
        
        // We synchronized /w the transactions so that they see the state change.
        synchronized (TRANSACTION_MUTEX) {
            // Re-checking since storing the batch may not be needed.
            if( (force && openBatch.base!=-1 ) || openBatch.pageCount() > updateBatchSize ) {
                batch = openBatch;
                openBatch = new Batch(batch.head);
                batches.addLast(openBatch);
            } else {
                return;
            }
        }
        
        // Write any outstanding deferred cache updates...
        batch.performDefferedUpdates(pageFile);

        // Link it to the last batch.
        batch.previous = lastBatchPage; 
        
        // Store the batch record.
        lastBatchPage = batch.page = storeObject(batch);
        LOG.trace("stored batch: "+batch);


        // Update the header to know about the new batch page.
        header.optimistic_recovery_page = batch.page;
        storeHeader();
    }
    
    /**
     * Performs a file sync. 
     * 
     * This allows two types of batch state changes to occur:
     * <ul>
     * <li> storing -> stored
     * <li> performed -> released
     * </ul>
     */
    private void syncBatches() {

        // This is a slow operation..
        if( synch ) {
            file.sync();
        }

        // Update the base_revision with the last performed revision.
        if (performedBatches!=storedBatches) {
            Batch lastPerformedBatch = storedBatches.getPrevious();
            header.base_revision = lastPerformedBatch.head;
        }

        // Were there some batches in the stored state?
        if (storingBatches!=openBatch) {
            
            // The last stored is actually synced now..
            Batch lastStoredBatch = openBatch.getPrevious();
            // Let the header know about it..
            header.pessimistic_recovery_page = lastStoredBatch.page;
            if( header.optimistic_recovery_page == header.pessimistic_recovery_page ) {
                header.optimistic_recovery_page = -1;
            }
            
            // We synchronized /w the transactions so that they see the state change.
            synchronized (TRANSACTION_MUTEX) {
                // Transition stored -> synced.
                storingBatches = openBatch;
            }
        }
        
        // Once a batch has been performed, subsequently synced, and no longer referenced,
        // it's allocated recovery space can be released.
        while( performedBatches!=storedBatches ) {
            if( performedBatches.snapshots!=0 ) {
                break;
            }
            
            if( performedBatches.page == header.pessimistic_recovery_page ) {
                header.pessimistic_recovery_page = -1;
            }

            // Free the update pages associated with the batch.
            performedBatches.release(allocator);
            
            // Free the batch record itself.
            Extent.free(pageFile, performedBatches.page);
            
            // don't need to sync /w transactions since they don't use the performedBatches variable.
            // Transition performed -> released
            performedBatches = performedBatches.getNext();
            
            // removes the released batch form the batch list.
            performedBatches.getPrevious().unlink();
        }

        // Store the free list..
        int previousFreeListPage = header.free_list_page;
        header.free_list_page = storeObject(storedFreeList);
        storeHeader();

        // Release the previous free list.
        if (previousFreeListPage >= 0) {
            Extent.free(pageFile, previousFreeListPage);
        }
    }

    /**
     * Attempts to perform a batch state change: stored -> performed
     * 
     * Once a batch is performed, new snapshots will not reference 
     * the batch anymore.
     */
    public void performBatches() {

        if( storedBatches==storingBatches ) {
            // There are no batches in the synced state for use to transition.
            return;
        }
              
        // The last performed batch MIGHT still have an open snapshot.
        // we can't transition from synced, until that snapshot closes.
        Batch lastPerformed = storedBatches.getPrevious();
        if( lastPerformed!=null && lastPerformed.snapshots!=0) {
            return;
        }
        
        while( storedBatches!=storingBatches ) {

            LOG.trace("Performing batch: "+storedBatches);

            // Performing the batch actually applies the updates to the original page locations.
            for (Commit commit : storedBatches) {
                for (Entry<Integer, Update> entry : commit.updates.entrySet()) {
                    int page = entry.getKey();
                    Update update = entry.getValue();

                    // is it a shadow update?
                    if( page != update.page ) {
                        
                        if( storedBatches.recovered ) {
                            // If we are recovering, the allocator MIGHT not have the shadow
                            // page as being allocated.  This makes sure it's allocated so that
                            // new transaction to get this page and overwrite it in error.
                            allocator.unfree(update.page, 1);
                        }
                        
                        // Perform the update by copying the updated page the original
                        // page location.
                        ByteBuffer slice = pageFile.slice(SliceType.READ, update.page, 1);
                        try {
                            pageFile.write(page, slice);
                        } finally { 
                            pageFile.unslice(slice);
                        }
                        
                    }
                    if( update.wasAllocated() ) {
                        
                        if( storedBatches.recovered ) {
                            // If we are recovering, the allocator MIGHT not have this 
                            // page as being allocated.  This makes sure it's allocated so that
                            // new transaction to get this page and overwrite it in error.
                            allocator.unfree(page, 1);
                        }
                        // Update the persistent free list.  This gets stored on the next sync.
                        storedFreeList.remove(page, 1);
                        
                    } else if( update.wasFreed() ) {
                        storedFreeList.add(page, 1);
                    }

                    // update the read cache..
                    DeferredUpdate du = update.deferredUpdate();
                    if( du != null ) {
                        if( du.wasDeferredClear() ) {
                            readCache.map.remove(page);
                        } else if( du.wasDeferredStore() ) {
                            readCache.map.put(page, du.value);
                        }
                    }

                }
            }
            
            storedBatches.performed = true;
            
            // We synchronized /w the transactions so that they see the state change.
            synchronized (TRANSACTION_MUTEX) {
                // Transition synced -> performed
                storedBatches = storedBatches.getNext();
            }
            
            lastPerformed = storedBatches.getPrevious();
            // We have to stop if the last batch performed has an open snapshot.
            if( lastPerformed.snapshots!=0 ) {
                break;
            }
        }
    }
    
    // /////////////////////////////////////////////////////////////////
    // Snapshot management
    // /////////////////////////////////////////////////////////////////
    
    Snapshot openSnapshot() {
        synchronized(TRANSACTION_MUTEX) {

            // re-use the last entry if it was a snapshot head..
            Commit commit = openBatch.getHeadCommit();
            SnapshotTracker tracker = null;

            if( commit !=null ) {
                if( commit.snapshotTracker == null ) {
                    // So we can track the new snapshot...
                    commit.snapshotTracker = new SnapshotTracker(openBatch, commit);
                }
                tracker = commit.snapshotTracker;
            } else {
                tracker = new SnapshotTracker(openBatch, null);
            }

            // Open the snapshot
            return new Snapshot(this, tracker, storedBatches).open();
        }
    }
    
    // /////////////////////////////////////////////////////////////////
    // TODO:
    // /////////////////////////////////////////////////////////////////
    
    /**
     * The quiesce method is used to pause/stop access to the concurrent page file.
     * access can be restored using the {@link #resume()} method.    
     * 
     * @param reads if true, the suspend will also suspend read only transactions. 
     * @param blocking if true, transactions will block until the {@link #resume()} method 
     *          is called, otherwise they will receive errors.
     * @param drain if true, in progress transactions are allowed to complete, otherwise they
     *        also are suspended. 
     */
    public void suspend(boolean reads, boolean blocking, boolean drain) {
    }

    /**
     * Resumes a previously suspended page file. 
     */
    public void resume() {
    }
    
    
    // /////////////////////////////////////////////////////////////////
    // Helper methods
    // /////////////////////////////////////////////////////////////////
    
    private int storeObject(Object value) {
        try {
            ExtentOutputStream eos = new ExtentOutputStream(pageFile);
            ObjectOutputStream oos = new ObjectOutputStream(eos);
            oos.writeObject(value);
            oos.close();
            return eos.getPage();
        } catch (IOException e) {
            throw new IOPagingException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T loadObject( int pageId ) {
        try {
            ExtentInputStream eis = new ExtentInputStream(pageFile, pageId);
            ObjectInputStream ois = new ObjectInputStream(eis);
            return (T) ois.readObject();
        } catch (IOException e) {
            throw new IOPagingException(e);
        } catch (ClassNotFoundException e) {
            throw new IOPagingException(e);
        }
    }
    
    private void storeHeader() {
        LOG.trace("storing file header: "+header);
        file.write(0, header.encode());
    }    
    
    // /////////////////////////////////////////////////////////////////
    // Simple Helper Classes
    // /////////////////////////////////////////////////////////////////

    final class ReadCache {
        public final Map<Integer, Object> map;

        public ReadCache(int size) {
            map = Collections.synchronizedMap(new LRUCache<Integer, Object>(size));
        }

        @SuppressWarnings("unchecked") <T> T cacheLoad(PagedAccessor<T> marshaller, int pageId) {
            T rc = (T) map.get(pageId);
            if( rc ==null ) {
                rc = marshaller.load(pageFile, pageId);
                map.put(pageId, rc);
            }
            return rc;
        }

        public void clear() {
            map.clear();
        }        
    }    
}
