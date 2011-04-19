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

import java.io.*;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.fusesource.hawtdb.internal.io.MemoryMappedFileFactory;
import org.fusesource.hawtdb.internal.page.ExtentInputStream;
import org.fusesource.hawtdb.internal.page.ExtentOutputStream;
import org.fusesource.hawtdb.internal.page.HawtPageFile;
import org.fusesource.hawtdb.internal.util.Ranges;

/**
 * A factory to create PageFile objects.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PageFileFactory {

    static final AsciiBuffer HAWT_DB_PAGE_FILE_MAGIC;
    static {
        try {
            HAWT_DB_PAGE_FILE_MAGIC = new AsciiBuffer("HawtDB/PageFile/1.0".getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private final MemoryMappedFileFactory mappedFileFactory = new MemoryMappedFileFactory();
    private HawtPageFile pageFile;

    protected int headerSize = 0;
    protected short pageSize = 512;
    protected int maxPages = Integer.MAX_VALUE;
    protected boolean storeFreePages = true;

    public PageFile getPageFile() {
        return pageFile;
    }

    /**
     * Opens the PageFile object. A subsequent call to {@link #getPageFile()} will return 
     * the opened PageFile.
     */
    public void open() {

        if (pageFile == null) {
            if( pageSize <= 0 ) {
                throw new IllegalArgumentException("pageSize property must be greater than 0");
            }
            if( maxPages <= 0 ) {
                throw new IllegalArgumentException("maxPages property must be greater than 0");
            }
            if( headerSize < 0 ) {
                throw new IllegalArgumentException("headerSize property cannot be negative.");
            }
            if( storeFreePages && headerSize==0 ) {
                headerSize = 512;
            }
            try {

                boolean recover = storeFreePages && getFile().exists() && getFile().length() > 512;

                try {
                    mappedFileFactory.open();
                } catch (IOException e) {
                    throw new IOPagingException(e);
                }

                int freePageExtent = -1;
                if( storeFreePages ) {
                    if( recover ) {
                        Buffer header = new Buffer(512);
                        mappedFileFactory.getMemoryMappedFile().read(0, header);
                        DataByteArrayInputStream his = new DataByteArrayInputStream(header);

                        Buffer magic = new Buffer(HAWT_DB_PAGE_FILE_MAGIC.length());
                        his.readFully(magic.data, magic.offset, magic.length());
                        if( !magic.ascii().equals(HAWT_DB_PAGE_FILE_MAGIC)) {
                            throw new IOPagingException("File's magic does not match expected value");
                        }
                        freePageExtent = his.readInt();
                        headerSize = his.readInt();
                        pageSize = his.readShort();
                    } else {
                    }
                }

                pageFile = new HawtPageFile(mappedFileFactory.getMemoryMappedFile(), pageSize, headerSize, maxPages, storeFreePages);

                if( freePageExtent >=0 ) {
                    DataInputStream is = new DataInputStream(new ExtentInputStream(pageFile, freePageExtent));
                    pageFile.allocator().getFreeRanges().readExternal(is);
                    is.close();
                }

                if( pageFile.storeFreePages && !isReadOnly() ) {
                    writePageFileHeader(-1);
                }


            } catch (IOException e) {
                throw new IOPagingException(e);
            }
        }
    }

    private void writePageFileHeader(int freePageExtent) throws IOException {
        DataByteArrayOutputStream os = new DataByteArrayOutputStream();
        os.write(HAWT_DB_PAGE_FILE_MAGIC);
        os.writeInt(freePageExtent);
        os.writeInt(headerSize);
        os.writeShort(pageSize);
        os.close();
        mappedFileFactory.getMemoryMappedFile().write(0, os.toBuffer());
    }

    /**
     * Closes the previously opened PageFile object.  Subsequent calls to 
     * {@link PageFileFactory#getPageFile()} will return null. 
     */
    public void close() throws IOException {
        if (pageFile != null) {
            pageFile.flush();
            if( !isReadOnly() && pageFile.storeFreePages ) {
                Ranges ranges = pageFile.allocator().getFreeRanges().copy();
                int freePageExtent = pageFile.alloc();
                DataOutputStream os = new DataOutputStream(new ExtentOutputStream(pageFile, freePageExtent, (short)1, (short)200));
                ranges.writeExternal(os);
                os.close();
                writePageFileHeader(freePageExtent);
            }
            pageFile.flush();
            pageFile = null;
        }        
        mappedFileFactory.close();
    }

    public int getHeaderSize() {
        return headerSize;
    }
    public void setHeaderSize(int headerSize) {
        this.headerSize = headerSize;
    }

    public short getPageSize() {
        return pageSize;
    }
    public void setPageSize(short pageSize) {
        this.pageSize = pageSize;
    }

    public int getMaxPages() {
        return maxPages;
    }
    public void setMaxPages(int maxPages) {
        this.maxPages = maxPages;
    }
    public void setMaxFileSize(long size) {
        setMaxPages( (int)((size-getHeaderSize())/getPageSize()) );
    }
    
    public File getFile() {
        return mappedFileFactory.getFile();
    }

    public int getMappingSegementSize() {
        return mappedFileFactory.getMappingSegementSize();
    }

    public void setFile(File file) {
        mappedFileFactory.setFile(file);
    }

    public void setMappingSegementSize(int mappingSegementSize) {
        mappedFileFactory.setMappingSegementSize(mappingSegementSize);
    }

    public boolean isStoreFreePages() {
        return storeFreePages;
    }

    public void setStoreFreePages(boolean storeFreePages) {
        this.storeFreePages = storeFreePages;
    }

    public boolean isReadOnly() {
        return mappedFileFactory.isReadOnly();
    }

    public void setReadOnly(boolean readOnly) {
        mappedFileFactory.setReadOnly(readOnly);
    }
}
