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

import java.util.Map;

import org.fusesource.hawtdb.api.PagedAccessor;

/**
 * 
 * A SnapshotTracker  tracks the open snapshots opened on a given
 * commit.  This is what allows snapshots/transactions to get a point
 * in time view of the page file.
 *  
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final class SnapshotTracker {

    final Batch parentBatch;
    final Commit parentCommit;
    final long headRevision;
    /** The number of times this snapshot has been opened. */
    protected volatile int snapshots;
    
    public SnapshotTracker(Batch parentBatch, Commit parentCommit) {
        this.parentBatch = parentBatch;
        this.parentCommit = parentCommit;
        Commit lastEntry = this.parentBatch.commits.getTail();
        this.headRevision = (lastEntry == null ? this.parentBatch.head : lastEntry.getHeadRevision())+1;
    }
    
    public String toString() { 
        return "{ references: "+this.snapshots+" }";
    }

    public long getHeadRevision() {
        return headRevision;
    }

    public int translatePage(int page) {
        if( parentCommit == null ) {
            return page;
        }

        int rc = page;
        // Look for the page in the previous commits..
        Batch batch = parentBatch;
        Commit commit = parentCommit;
        outer: while( true ) {
            if( batch.isPerformed() ) {
                break;
            }
            
            Commit first = null;
            while( true ) {
                if( first == null ) {
                    first = commit;
                } else if( first==commit ) {
                    break;
                }
                
                Update update = commit.updates.get(rc);
                if( update!=null ) {
                    if( update.shadowed() ) {
                        rc = update.shadow();
                    }
                    break outer;
                }
                commit = commit.getPreviousCircular();
            }
            
            batch = batch.getPrevious();
            if( batch==null ) {
                break;
            }
            commit = batch.commits.getTail();
        }
        return rc;
    }
    
    
    public <T> T get(PagedAccessor<T> marshaller, int page) {
        if( parentCommit == null ) {
            return null;
        }

        Batch batch = parentBatch;
        Commit commit = parentCommit;


        while( true ) {
            if( batch.isPerformed() ) {
                break;
            }
            
            Commit tail = null;
            while( true ) {
                if( tail == null  ) {
                    tail = commit;
                } else if( tail==commit ) {
                    break;
                }

                Update update = commit.updates.get(page);
                if( update!=null ) {
                    DeferredUpdate du  = update.deferredUpdate();
                    if (du!=null) {
                        return du.<T>value();
                    }
                }
                commit = commit.getPreviousCircular();
            }

            batch = batch.getPrevious();
            if( batch==null ) {
                break;
            }
            commit = batch.commits.getTail();
        }
        return null;
    }

    public long commitCheck(Map<Integer, Update> pageUpdates) {
        long rc= parentBatch.head;
        Batch batch = parentBatch;
        Commit commit = parentCommit==null ? batch.commits.getHead() : parentCommit.getNext();

        while( true ) {
            while( commit!=null ) {
                rc = commit.commitCheck(pageUpdates);
                commit = commit.getNext();
            }

            batch = batch.getNext();
            if( batch==null ) {
                break;
            }
            commit = batch.commits.getHead();
        }
        return rc;
    }

}