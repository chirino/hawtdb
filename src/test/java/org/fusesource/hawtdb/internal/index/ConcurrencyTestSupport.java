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
package org.fusesource.hawtdb.internal.index;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.fusesource.hawtdb.api.TxPageFile;
import org.fusesource.hawtdb.api.TxPageFileFactory;
import org.fusesource.hawtdb.api.Index;
import org.fusesource.hawtdb.api.OptimisticUpdateException;
import org.fusesource.hawtdb.api.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public abstract class ConcurrencyTestSupport {

    protected volatile TxPageFileFactory pageFactory;
    protected volatile TxPageFile pageFile;

    @Before
    public void setUp() throws Exception {
        pageFactory = createConcurrentPageFileFactory();
        pageFactory.getFile().delete();
        pageFactory.open();
        pageFile = pageFactory.getTxPageFile();
        createIndex();
    }

    @After
    public void tearDown() throws Exception {
        if (pageFile != null) {
            pageFactory.close();
            pageFactory = null;
        }
    }

    @Test
    public void testIsolationInConcurrentReadWriteTransactions() throws Exception {
        final AtomicReference error = new AtomicReference();
        final CountDownLatch mutationLatch = new CountDownLatch(1);
        final CountDownLatch preCommitLatch = new CountDownLatch(1);
        final CountDownLatch commitLatch = new CountDownLatch(2);
        ExecutorService executor = Executors.newCachedThreadPool();
        //
        executor.submit(new Runnable() {

            public void run() {
                try {
                    Transaction writer = pageFile.tx();
                    Index<String, Long> index = openIndex(writer);
                    try {
                        for (int i = 0; i < 1000; i++) {
                            index.put("" + i, Long.valueOf(i));
                        }
                        mutationLatch.countDown();
                        if (preCommitLatch.await(60, TimeUnit.SECONDS)) {
                            writer.commit();
                        } else {
                            throw new RuntimeException();
                        }
                    } catch (InterruptedException ex) {
                        error.set(ex);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    error.compareAndSet(null, ex);
                } finally {
                    commitLatch.countDown();
                }
            }

        });
        //
        executor.submit(new Runnable() {

            public void run() {
                try {
                    Transaction reader = pageFile.tx();
                    Index<String, Long> index = openIndex(reader);
                    try {
                        if (mutationLatch.await(60, TimeUnit.SECONDS)) {
                            for (int i = 0; i < 1000; i++) {
                                if (index.get("" + i) != null) {
                                    error.set(new RuntimeException("Bad transaction isolation!"));
                                    throw new RuntimeException();
                                }
                            }
                            reader.commit();
                            preCommitLatch.countDown();
                        } else {
                            throw new RuntimeException();
                        }
                    } catch (InterruptedException ex) {
                        error.set(ex);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    error.compareAndSet(null, ex);
                } finally {
                    commitLatch.countDown();
                }
            }

        });
        assertTrue(commitLatch.await(60, TimeUnit.SECONDS));
        if (error.get() == null) {
            Transaction checker = pageFile.tx();
            Index<String, Long> index = openIndex(checker);
            for (int i = 0; i < 1000; i++) {
                assertEquals(Long.valueOf(i), index.get("" + i));
            }
            checker.commit();
        } else {
            throw (Exception) error.get();
        }
        //
        executor.shutdownNow();
    }

    @Test
    public void testConflictResolutionInConcurrentWriteTransactions() throws Exception {
        final AtomicReference error = new AtomicReference();
        final CountDownLatch preCommitLatch = new CountDownLatch(2);
        final CountDownLatch commitLatch = new CountDownLatch(2);
        ExecutorService executor = Executors.newCachedThreadPool();
        //
        executor.submit(new Runnable() {

            public void run() {
                try {
                    Transaction writer = pageFile.tx();
                    Index<String, Long> index = openIndex(writer);
                    try {
                        for (int i = 0; i < 1000; i++) {
                            index.put("" + i, Long.valueOf(i));
                        }
                        preCommitLatch.countDown();
                        if (preCommitLatch.await(10, TimeUnit.SECONDS)) {
                            try {
                                writer.commit();
                                System.out.println("Committed from 1.");
                            } catch (OptimisticUpdateException ex) {
                                System.out.println("Replaying from 1...");
                                run();
                            }
                        } else {
                            throw new RuntimeException();
                        }
                    } catch (InterruptedException ex) {
                        error.set(ex);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    error.compareAndSet(null, ex);
                } finally {
                    commitLatch.countDown();
                }
            }

        });
        //
        executor.submit(new Runnable() {

            public void run() {
                try {
                    Transaction writer = pageFile.tx();
                    Index<String, Long> index = openIndex(writer);
                    try {
                        for (int i = 1000; i < 2000; i++) {
                            index.put("" + i, Long.valueOf(i));
                        }
                        preCommitLatch.countDown();
                        if (preCommitLatch.await(10, TimeUnit.SECONDS)) {
                            try {
                                writer.commit();
                                System.out.println("Committed from 2.");
                            } catch (OptimisticUpdateException ex) {
                                System.out.println("Replaying from 2...");
                                run();
                            }
                        } else {
                            throw new RuntimeException();
                        }
                    } catch (InterruptedException ex) {
                        error.set(ex);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    error.compareAndSet(null, ex);
                } finally {
                    commitLatch.countDown();
                }
            }

        });
        assertTrue(commitLatch.await(60, TimeUnit.SECONDS));
        if (error.get() == null) {
            Transaction checker = pageFile.tx();
            Index<String, Long> index = openIndex(checker);
            for (int i = 0; i < 2000; i++) {
                assertEquals(Long.valueOf(i), index.get("" + i));
            }
            checker.commit();
        } else {
            throw (Exception) error.get();
        }
        //
        executor.shutdownNow();
    }

    protected TxPageFileFactory createConcurrentPageFileFactory() throws IOException {
        TxPageFileFactory rc = new TxPageFileFactory();
        rc.setFile(File.createTempFile(getClass().getName(), ".db"));
        return rc;
    }

    abstract protected Index<String, Long> createIndex();

    abstract protected Index<String, Long> openIndex(Transaction tx);

}
