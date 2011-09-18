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

import java.io.File;
import java.io.IOException;

import org.fusesource.hawtdb.internal.page.HawtPageFile;
import org.fusesource.hawtdb.internal.page.HawtTxPageFile;
import org.fusesource.hawtdb.internal.page.PageCache;
import org.fusesource.hawtdb.internal.page.ThreadLocalLFUPageCache;

/**
 * A factory to create TxPageFile objects.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TxPageFileFactory {

    private final PageFileFactory pageFileFactory = new PageFileFactory();
    private HawtTxPageFile txPageFile;
    protected boolean drainOnClose;
    protected boolean sync = true;
    protected boolean useWorkerThread;
    private PageCache pageCache = new ThreadLocalLFUPageCache(1024, 0.5f);

    public TxPageFileFactory() {
        pageFileFactory.setHeaderSize(HawtTxPageFile.FILE_HEADER_SIZE);
        pageFileFactory.setStoreFreePages(false);
    }

    /**
     * Opens the TxPageFile object. A subsequent call to {@link #getTxPageFile()} will return 
     * the opened TxPageFile.
     */
    public void open() {
        if (getFile() == null) {
            throw new IllegalArgumentException("file property not set");
        }
        boolean existed = getFile().isFile();
        pageFileFactory.open();
        if (txPageFile == null) {
            txPageFile = new HawtTxPageFile(this, (HawtPageFile) pageFileFactory.getPageFile());
            if (existed) {
                txPageFile.recover();
            } else {
                txPageFile.reset();
            }
        }
    }

    /**
     * Closes the previously opened PageFile object.  Subsequent calls to 
     * {@link TxPageFileFactory#getTxPageFile()} will return null.
     */
    public void close() throws IOException {
        if (txPageFile != null) {
            txPageFile.close();
            txPageFile = null;
        }
        pageFileFactory.close();
    }

    public boolean isSync() {
        return sync;
    }

    public void setSync(boolean sync) {
        this.sync = sync;
    }

    public TxPageFile getTxPageFile() {
        return txPageFile;
    }

    public boolean isDrainOnClose() {
        return drainOnClose;
    }

    public void setDrainOnClose(boolean drainOnClose) {
        this.drainOnClose = drainOnClose;
    }

    public boolean isUseWorkerThread() {
        return useWorkerThread;
    }

    public void setUseWorkerThread(boolean useWorkerThread) {
        this.useWorkerThread = useWorkerThread;
    }

    public File getFile() {
        return pageFileFactory.getFile();
    }

    public int getMappingSegementSize() {
        return pageFileFactory.getMappingSegementSize();
    }

    public int getMaxPages() {
        return pageFileFactory.getMaxPages();
    }

    public short getPageSize() {
        return pageFileFactory.getPageSize();
    }

    public void setFile(File file) {
        pageFileFactory.setFile(file);
    }

    public void setMappingSegementSize(int mappingSegementSize) {
        pageFileFactory.setMappingSegementSize(mappingSegementSize);
    }

    public void setMaxFileSize(long size) {
        pageFileFactory.setMaxFileSize(size);
    }

    public void setMaxPages(int maxPages) {
        pageFileFactory.setMaxPages(maxPages);
    }

    public void setPageSize(short pageSize) {
        pageFileFactory.setPageSize(pageSize);
    }

    public PageCache getPageCache() {
        return pageCache;
    }

    public void setPageCache(PageCache pageCache) {
        this.pageCache = pageCache;
    }

}
