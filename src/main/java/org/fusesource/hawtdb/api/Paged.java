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

import java.nio.ByteBuffer;

import org.fusesource.hawtbuf.Buffer;

/**
 * Implemented by objects which provides block io access to pages on file.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface Paged {

    /**
     * @return An object which provides access to allocate/deallocate pages.
     */
    Allocator allocator();
    
    /**
     * does the same as allocator().alloc(1)
     * @return a newly allocated page location.
     */
    int alloc();

    /**
     * does the same as allocator().free(page, 1)
     */
    void free(int page);

    enum SliceType {
        READ, WRITE, READ_WRITE
    }

    /**
     * Provides direct access to the memory associated with a page. Specifying
     * the correct mode argument is especially critical and the Paged resources
     * is being accessed in a transaction context so that the transaction can
     * maintain snapshot isolation.
     * 
     * @param mode
     *            how will the buffer be used.
     * @param pageId
     *            the starting page of the buffer
     * @param count
     *            the number of pages to include in the buffer.
     * @return
     * @throws IOPagingException
     */
    public ByteBuffer slice(SliceType mode, int pageId, int count) throws IOPagingException;

    public void unslice(ByteBuffer buffer);

    /**
     * Copies the contents of a page into the buffer space. The buffer offset
     * will be updated to reflect the amount of data copied into the buffer.
     * 
     * @param pageId
     * @param buffer
     */
    public void read(int pageId, Buffer buffer);

    /**
     * Copies the buffer into the page. The buffer offset will be updated to
     * reflect the amount of data copied to the page.
     * 
     * @param pageId
     * @param buffer
     */
    public void write(int pageId, Buffer buffer);
    
    /**
     * @return the maximum number of bytes that be read or written to a page.
     */
    int getPageSize();

    /**
     * @return the number of pages that would be required to store the specified
     *         number of bytes
     */
    int pages(int length);
    
    void flush();
    
    
    /**
     * Gets an object previously put at the given page.  The returned object SHOULD NEVER be mutated.
     *  
     * @param page
     * @return
     */
    <T> T get(PagedAccessor<T> pagedAccessor, int page);
    
    /**
     * Put an object at a given page.  The supplied object SHOULD NEVER be mutated 
     * once it has been stored.
     * 
     * @param page
     * @param value
     */
    <T> void put(PagedAccessor<T> pagedAccessor, int page, T value);
    
    /**
     * Frees any pages associated with the value stored at the given page if any.  Does not free
     * the page supplied.
     *  
     * @param page
     * @return
     */
    <T> void clear(PagedAccessor<T> pagedAccessor, int page);

}
