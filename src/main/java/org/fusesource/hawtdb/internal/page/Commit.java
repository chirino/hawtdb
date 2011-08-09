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

import org.fusesource.hawtdb.api.Allocator;
import org.fusesource.hawtdb.api.OptimisticUpdateException;
import org.fusesource.hawtdb.util.list.LinkedNode;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import static org.fusesource.hawtdb.internal.page.Logging.trace;
import static org.fusesource.hawtdb.internal.page.Logging.traced;

/**
 * Tracks the updates that were part of a transaction commit.
 * Multiple commit objects can be merged into a single commit.
 * 
 * A Commit is a BatchEntry and stored in Batch object.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final class Commit extends LinkedNode<Commit> implements Externalizable {

    /** Tracks open snapshots against this commit */
    volatile SnapshotTracker snapshotTracker;

    /** oldest revision in the commit range. */
    private volatile long base;
    /** newest revision in the commit range, will match base if this only tracks one commit */ 
    private volatile long head;
    
    /** all the page updates that are part of the redo */
    volatile ConcurrentHashMap<Integer, Update> updates;

    public Commit() {
    }
    
    public Commit(long version, ConcurrentHashMap<Integer, Update> updates) {
        this.head = this.base = version;
        this.updates = updates;
    }
    
    
    public long getHeadRevision() {
        return head;
    }
    
    public String toString() {
        int updateSize = updates==null ? 0 : updates.size();
        return "{ base: "+this.base+", head: "+this.head+", updates: "+updateSize+" }";
    }

    public long commitCheck(Map<Integer, Update> newUpdate) {
        for (Integer page : newUpdate.keySet()) {
            if( updates.containsKey( page ) ) {
                throw new OptimisticUpdateException();
            }
        }
        return head;
    }

    public void merge(Allocator allocator, long rev, ConcurrentHashMap<Integer, Update> updates) {
        assert head+1 == rev;
        head=rev;
        // merge all the entries in the update..
        for (Entry<Integer, Update> entry : updates.entrySet()) {
            merge(allocator, entry.getKey(), entry.getValue());
            if( traced(entry.getKey()) ) {
                trace("merged: %s", entry);
            }
        }
        assert(stillSane());
    }

    /**
     * merges one update..
     * 
     * @param page
     * @param update
     */
    void merge(Allocator allocator, int page, Update update) {
        assert !(update.allocated() && update.shadowed() && update.freed()) : "This update can't be in multiple states";

        Update previous = this.updates.put(page, update);
        if (previous != null) {
            previous.history.addAll(update.history);
            update.history = previous.history;
            if( update.freed() ) {

                assert !previous.freed(): "free can not follow a free.";

                // yes the previous page can be allocated and shadowed since
                // a shadow update can be merged onto a an allocation

                if( previous.shadowed() ) {
                    update.note("free previous shadow: "+previous.shadow());
                    allocator.free(previous.shadow(), 1);
                }


                if( previous.allocated() ) {
                    allocator.free(page, 1);

                    // in this case the update is canceled out since the
                    // page never made it to disk.
                    this.updates.remove(page);
                }


            } else if(update.allocated()) {

                if( !previous.freed() ) {
                    assert previous.freed(): "allocation updates can only follow freed updates.";
                }
                assert !(previous.allocated() && previous.shadowed()): "Unexpected previous state.";

                if( !update.put() ) {
                    // in this case the update cancels out.
                    this.updates.remove(page);
                }

            } else if(update.shadowed()) {

                // Yes.. it's possible to be allocated and then shadowed.
                if( previous.allocated() ) {
                    update.allocated(true);
                }

                if( previous.shadowed() ) {
                    update.note("free previous shadow: "+previous.shadow());
                    allocator.free(previous.shadow(), 1);
                }

            } else if( update.deferredUpdate()!=null ) {
                assert !previous.shadowed() : "deferred updates should not have shadows assigned.";

                if( previous.allocated() ) {
                    update.allocated(true);
                }
            } else {
                throw new AssertionError("Unexpected update state");
            }
        }
    }

    public boolean stillSane() {
        for (Entry<Integer, Update> entry : updates.entrySet()) {
            int page = entry.getKey();
            Update update = entry.getValue();

            // is a shadow update?
            if( update.shadowed() ) {
                final Update badboy = updates.get(update.shadow());
                if( badboy !=null ) {
                    throw new AssertionError("a normal page ("+page+") is also being used as a shadow page ("+update.shadow() +").");
                }
            }

        }
        return true;
    }

    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        base = in.readLong();
        head = in.readLong();
        updates = (ConcurrentHashMap<Integer, Update>) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(base);
        out.writeLong(head);
        out.writeObject(updates);
    }

}