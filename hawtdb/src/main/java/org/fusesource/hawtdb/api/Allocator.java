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

import org.fusesource.hawtdb.internal.util.Ranges;

/**
 * Handles allocation management of resources.  Used for page allocations
 * in a {@link Paged} resource.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface Allocator { 

    /**
     * Allocates a continuous number of items and returns the position of first item in the sequence.
     */
    public int alloc(int count) throws OutOfSpaceException;

    /**
     * Frees a given number of items at a given position.
     */
    public void free(int firstPage, int count);

    /**
     * Undoes a previous free method call.
     * 
     * optional method. implementations my throw UnsupportedOperationException
     * @throws UnsupportedOperationException may be thrown by some allocators.
     */
    public void unfree(int firstPage, int count) throws UnsupportedOperationException;

    /**
     * Frees all previous allocations.
     * 
     * optional method. implementations my throw UnsupportedOperationException
     * @throws UnsupportedOperationException 
     */
    public void clear() throws UnsupportedOperationException;

    /**
     * @return the maximum number of pages that this allocator will allocate.
     */
    public int getLimit();

    /**
     * @param page
     * @return true if the page has been allocated.
     */
    public boolean isAllocated(int page);

    /**
     * 
     */
    public void setFreeRanges(Ranges freeList);

    /**
     *
     */
    public Ranges getFreeRanges();
}