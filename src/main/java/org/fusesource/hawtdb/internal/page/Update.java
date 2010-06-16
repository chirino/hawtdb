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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

/**
 * <p>Tracks a page update.
 * </p>
 * <p>
 * To be able to provide snapshot isolation and to make 
 * sure a set of updates can be performed atomically,  updates
 * to existing pages are stored in a shadow page.  Once all the updates
 * that are part of the transaction have been verified to be on disk,
 * and no open snapshot would need to access the data on the original page,
 * the contents of the shadow page are copied to the original page location
 * and the shadow page gets freed.
 * </p>
 * <p>
 * A Update object is stored in the updates map in a Commit object.  That map
 * is keyed off the original page location.  The Update page is the location
 * of the shadow page.
 * </p>
 * <p>
 * Updates to pages which were allocated in the same transaction get done
 * directly against the allocated page since no snapshot would have a view onto 
 * that page.  In this case Update is not assigned a shadow page.
 * </p>
 * <p>
 * An update maintains some bit flags to know if the page was a new allocation
 * or if the update was just freeing a previously allocated page, etc.  This data
 * is used to properly maintain the persisted free page list.
 * </p>
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Update implements Externalizable {
    
    public static final byte PAGE_ALLOCATED   = 0x01 << 0;
    public static final byte PAGE_FREED       = 0x01 << 1;
    public static final byte PAGE_PUT         = 0x01 << 2;
    public static final byte PAGE_REMOVE      = 0x01 << 3;

    private static final long serialVersionUID = -1128410792448869134L;
    
    byte flags;
    int shadow = -1;

    ArrayList<String> history = new ArrayList<String>();

    public Update note(String value) {
        history.add(value);
        return this;
    }

    public Update() {
    }
    
    public Update(Update update) {
        this.shadow = update.shadow;
        this.flags = (byte) (update.flags & (PAGE_ALLOCATED|PAGE_FREED));
        this.history = update.history;
    }

    public static Update update() {
        return new Update();
    }
    
    public static Update update(Update update) {
        return new Update(update);
    }

    public Update shadow(int value) {
        shadow = value;
        return this;
    }

    public int shadow() {
        assert !freed() && shadowed() : "The page does not have a shadow set.";
        return shadow;
    }

    boolean shadowed() {
        return shadow != -1;
    }

    public DeferredUpdate deferredUpdate() {
        return null;
    }
    
    public Update allocated(boolean value) {
        if( value ) {
            flags = (byte) ((flags & ~PAGE_FREED) | PAGE_ALLOCATED);
        } else {
            flags = (byte) (flags & ~PAGE_ALLOCATED);
        }
        return this;
    }
    
    public Update freed(boolean value) {
        if( value ) {
            flags = (byte) ((flags & ~PAGE_ALLOCATED) | PAGE_FREED);
        } else {
            flags = (byte) (flags & ~PAGE_FREED);
        }
        return this;
    }

    public boolean freed() {
        return (flags & PAGE_FREED)!=0 ;
    }
    public boolean allocated() {
        return (flags & PAGE_ALLOCATED)!=0;
    }

    public boolean put() {
        return (flags & PAGE_PUT)!=0 ;
    }
    public boolean removed() {
        return (flags & PAGE_REMOVE)!=0;
    }
    
    @Override
    public String toString() {
        return "{ shadow: "+ shadow +", flags: "+flags+", history: "+history+", deferred: "+(deferredUpdate()!=null)+" }";
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        shadow = in.readInt();
        flags = in.readByte();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(shadow);
        out.writeByte(flags);
    }

    public int translate(int page) {
        return shadowed() ? shadow : page;
    }
}