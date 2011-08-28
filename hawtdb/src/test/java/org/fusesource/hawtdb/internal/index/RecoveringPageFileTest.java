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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import org.fusesource.hawtbuf.codec.LongCodec;
import org.fusesource.hawtbuf.codec.StringCodec;
import org.fusesource.hawtdb.internal.page.ExtentInputStream;
import org.fusesource.hawtdb.internal.page.ExtentOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import org.fusesource.hawtdb.api.BTreeIndexFactory;
import org.fusesource.hawtdb.api.PageFile;
import org.fusesource.hawtdb.api.PageFileFactory;
import org.fusesource.hawtdb.api.SortedIndex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class RecoveringPageFileTest {

    private static final BTreeIndexFactory<Long,String> INDEX_FACTORY = new BTreeIndexFactory<Long,String>();
    static {
        INDEX_FACTORY.setKeyCodec(LongCodec.INSTANCE);
        INDEX_FACTORY.setValueCodec(StringCodec.INSTANCE);
        INDEX_FACTORY.setDeferredEncoding(false);
    }

    private PageFileFactory pff;
    private PageFile pf;
    SortedIndex<Long, String> index;


    protected PageFileFactory createConcurrentPageFileFactory() {
        PageFileFactory rc = new PageFileFactory();
        rc.setPageSize((short) 512);
        rc.setFile(new File("target/test-data/" + getClass().getName() + ".db"));
        return rc;
    }

    @Before
    public void setUp() throws Exception {
        pff = createConcurrentPageFileFactory();
        pff.getFile().delete();
        pff.open();
        pf = pff.getPageFile();

        index = INDEX_FACTORY.create(pf);
        assertEquals(0, index.getIndexLocation());
        pf.flush();
    }

    @After
    public void tearDown() throws Exception {
        pff.close();
    }

    protected void reload() throws IOException {
        pff.close();
        pff.open();
        pf = pff.getPageFile();
        index = INDEX_FACTORY.open(pf, 0);
    }


    @Test
    public void testAddRollback() throws IOException, ClassNotFoundException {

        // Create the root index at the root page.
        int MAX_KEY=1000;
        long putCounter=0;
        long removeCounter=0;

        final Random random = new Random(7777);

        for(int recoveryLoop=0; recoveryLoop < 30; recoveryLoop ++) {
            if( recoveryLoop%10 == 0 ) {
                System.out.println("at recovery loop: "+recoveryLoop+", puts: "+putCounter+", removes: "+removeCounter);
            }

            // validate that the number of keys in the map is what is expected.
            assertEquals(putCounter - removeCounter, index.size());

            // Do a bunch of transactions before reloading the page file.
            for(int txLoop=0; txLoop < 1000; txLoop ++) {

                // do a couple of random inserts and deletes.
                for(int keyLoop=0; keyLoop < 10; keyLoop ++) {

                    Long key = (long)random.nextInt(MAX_KEY);
                    String value = index.get(key);
                    if( value == null ) {
                        index.put(key, "value");
                        putCounter++;
                    } else {
                        index.remove(key);
                        removeCounter++;
                    }

                }

                // flush every 100 ops..
                if( txLoop%100 == 0 ) {
                    pf.flush();
                }
            }

            reload();
        }

        int mapSize = 0;


    }


}
