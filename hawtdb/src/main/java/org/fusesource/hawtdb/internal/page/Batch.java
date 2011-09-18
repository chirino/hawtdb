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
import org.fusesource.hawtdb.api.Paged;
import org.fusesource.hawtdb.util.list.LinkedNode;
import org.fusesource.hawtdb.util.list.LinkedNodeList;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static org.fusesource.hawtdb.internal.page.Logging.trace;
import static org.fusesource.hawtdb.internal.page.Logging.traced;
import static org.fusesource.hawtdb.internal.page.Update.update;

/**
 * Aggregates a group of commits so that they can be more efficiently
 * stored to disk.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final class Batch extends LinkedNode<Batch> implements Externalizable, Iterable<Commit> {

    private static final long serialVersionUID = 1188640492489990493L;
    
    /** the pageId that this redo batch is stored at */
    volatile int page=-1;
    /** points to a previous redo batch page */
    public volatile int previous=-1;
    /** was this object reloaded from disk? */
    volatile boolean recovered;
    
    /** the commits and snapshots in the redo */ 
    final LinkedNodeList<Commit> commits = new LinkedNodeList<Commit>();
    /** tracks how many snapshots are referencing the redo */
    volatile int snapshots;
    /** the oldest commit in this redo */
    public volatile long base=-1;
    /** the newest commit in this redo */
    public volatile long head;

    volatile boolean performed;

    volatile ArrayList<Runnable> flushCallbacks = new ArrayList<Runnable>();
    
    public Batch() {
    }
    
    public boolean isPerformed() {
        return performed;
    }

    public Batch(long head) {
        this.head = head;
    }

    public String toString() { 
        return "{ page: "+this.page+", base: "+base+", head: "+head+", snapshots: "+snapshots+", commits: "+ commits.size()+", previous: "+previous+" }";
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(head);
        out.writeLong(base);
        out.writeInt(previous);

        // Only need to store the commits.
        ArrayList<Commit> l = new ArrayList<Commit>();
        for (Commit commit : this) {
            l.add(commit);
        }
        out.writeObject(l);
    }

    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        head = in.readLong();
        base = in.readLong();
        previous = in.readInt();
        ArrayList<Commit> l = (ArrayList<Commit>) in.readObject();
        for (Commit commit : l) {
            commits.addLast(commit);
        }
    }        

    public int pageCount() {
        int rc = 0;
        for (Commit commit : this) {
            rc += commit.updates.size();
        }
        return rc;
    }

    public Commit getHeadCommit() {
        Batch b = this;
        while( b!=null ) {
            if( !b.commits.isEmpty() ) {
                return b.commits.getTail();
            }
            b = b.getPrevious();
        }
        return null;
    }
    
    public Iterator<Commit> iterator() {
        return new Iterator<Commit>() {
            Commit next = commits.getHead();
            Commit last;
            
            public boolean hasNext() {
                return next!=null;
            }

            public Commit next() {
                if( next==null ) {
                    throw new NoSuchElementException();
                }
                last = next;
                next = next.getNext();
                return last;
            }

            public void remove() {
//                if( last==null ) {
                    throw new IllegalStateException();
//                }
//                last.unlink();
            }
        };
    }

    public void performDeferredUpdates(Paged pageFile) {
        for (Commit commit : this) {
            assert(commit.stillSane());
            if( commit.updates != null ) {
                for (Entry<Integer, Update> entry : commit.updates.entrySet()) {

                    Integer page = entry.getKey();
                    DeferredUpdate du = entry.getValue().deferredUpdate();

                    if( du == null ) {
                        continue;
                    }

                    assert !du.shadowed() : "deferred update should not have a shadow page.";
                    if( du.removed() ) {
                        assert(!du.put());

                        List<Integer> freePages = du.marshaller.pagesLinked(pageFile, page);
                        for (Integer linkedPage : freePages) {
                            commit.merge(pageFile.allocator(), linkedPage, update().freed(true));
                        }
                    }

                    if( du.put() ) {
                        assert(!du.removed());

                        if( !du.allocated() ) {
                            // update has to occur on a shadow page.
                            du.shadow(pageFile.allocator().alloc(1));

                            // free up the linked pages of the previous put
                            List<Integer> freePages = du.marshaller.pagesLinked(pageFile, page);
                            for (Integer linkedPage : freePages) {
                                commit.merge(pageFile.allocator(), linkedPage, update().freed(true));
                            }
                        }

                        List<Integer> linkedPages = du.marshaller.store(pageFile, du.translate(page), du.value);
                        if( traced(page) ) {
                            trace("storing update of %d at %d linked pages: %s", page, du.translate(page), linkedPages);
                        }
                        
                        for (Integer linkedPage : linkedPages) {
                            // add any allocated pages to the update list so that the free
                            // list gets properly adjusted.
                            commit.merge(pageFile.allocator(), linkedPage, update().allocated(true));
                        }
                    }
                }
            }
        }
    }

    public void release(Allocator allocator) {
        for (Commit commit : this) {
            for (Entry<Integer, Update> entry : commit.updates.entrySet()) {
                int key = entry.getKey();
                Update value = entry.getValue();

                if( value.freed() ) {
                    assert(!value.shadowed());
                    allocator.free(key, 1);
                }
                if( value.shadowed()) {
                    assert(!value.freed());
                    // need to free the shadow page..
                    allocator.free(value.shadow(), 1);
                }
            }
        }
    }

}