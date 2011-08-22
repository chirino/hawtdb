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

import org.fusesource.hawtbuf.codec.LongCodec;
import org.fusesource.hawtdb.api.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.fusesource.hawtdb.internal.page.LFUPageCache;
import static org.junit.Assert.assertEquals;


/**
 * Tests an Index
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class BTreeCacheTest {

    protected TxPageFileFactory pff;
    protected TxPageFile pf;
    protected BTreeIndexFactory<Long,Long> factory;


    protected TxPageFileFactory createConcurrentPageFileFactory() {
        TxPageFileFactory rc = new TxPageFileFactory();
        rc.setFile(new File("target/test-data/" + getClass().getName() + ".db"));
        rc.setPageSize((short) 512);
        rc.setPageCache(new LFUPageCache(1000, 0.5f));
        return rc;
    }

    @Before
    public void setup() {
        pff = createConcurrentPageFileFactory();
        pff.getFile().delete();
        pff.open();
        pf = pff.getTxPageFile();

        factory = new BTreeIndexFactory<Long,Long>();
        factory.setKeyCodec(LongCodec.INSTANCE);
        factory.setValueCodec(LongCodec.INSTANCE);
        factory.setDeferredEncoding(true);
    }

    @After
    public void tearDown() throws Exception {
        if( pf!=null ) {
            pff.close();
            pff = null;
        }
    }

    @Test
    public void testIndexOperations() throws Exception {

        Transaction tx = pf.tx();
        SortedIndex<Long, Long> index1 = factory.create(tx);
        final int page1 = index1.getIndexLocation();
        SortedIndex<Long, Long> index2 = factory.create(tx);
        final int page2 = index2.getIndexLocation();
        tx.commit();

        final AtomicLong current = new AtomicLong(-1);
        final AtomicLong gets = new AtomicLong(0);
        final AtomicLong puts = new AtomicLong(0);
        final AtomicLong removes = new AtomicLong(0);
        final AtomicBoolean done = new AtomicBoolean(false);

        ArrayList<Thread> threads = new ArrayList<Thread>();

        final long TARGET1_SIZE = 50000;
        final long TARGET2_SIZE = 70000;

        Thread thread = new Thread() {
            @Override
            public void run() {
                long i = 0;
                while(!done.get()) {
                    Transaction tx = pf.tx();
                    SortedIndex<Long, Long> index1 = factory.open(tx, page1);
                    SortedIndex<Long, Long> index2 = factory.open(tx, page2);

                    index1.put(i,i);
                    puts.incrementAndGet();
                    index2.put(i*2,i*2);
                    puts.incrementAndGet();

                    if( i > TARGET1_SIZE) {
                        long j = i- TARGET1_SIZE;
                        Long previous = index1.remove(j);
                        removes.incrementAndGet();
                        assertEquals((Long)j, previous);
                    }
                    tx.commit();

                    if( i > TARGET2_SIZE) {
                        long j = i - TARGET2_SIZE;
                        tx = pf.tx();
                        index1 = factory.open(tx, page1);
                        Long previous = index2.remove(j*2);
                        removes.incrementAndGet();
                        assertEquals((Long)(j*2), previous);
                        tx.commit();
                    }

                    current.set(i);
                    i++;
                }
            }
        };
        threads.add(thread);
        thread.start();

        for( int i=0; i < 4; i++ ) {
            Thread.sleep(1000*5);
            System.out.println("puts: "+puts.get()+", removes: "+removes.get());
        }
        done.set(true);

        for (Thread t : threads) {
            t.join();
        }

    }


}