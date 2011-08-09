/**
 *  Copyright (C) 2009, Progress Software Corporation and/or its
 * subsidiaries or affiliates.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.hawtdb.tests;

import org.fusesource.hawtbuf.codec.StringCodec;
import org.fusesource.hawtdb.api.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import org.junit.Ignore;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class GrowIssueTest {


    private final int size = 1024;
    private TxPageFileFactory factory;
    private TxPageFile file;

    @Before
    public void setUp() throws Exception {
        File f = new File("target/data/hawtdb.dat");
        f.delete();
        
        factory = new TxPageFileFactory();
        factory.setFile(f);

        factory.setMappingSegementSize(16 * 1024);
        // set 1mb as max file
        factory.setMaxFileSize(1024 * 1024);

        factory.open();
        file = factory.getTxPageFile();
    }

    @After
    public void tearDown() throws Exception {
        factory.close();
    }

    @Test
    public void testGrowIssue() throws Exception {

        // a 1kb string for testing
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < 1024; i++) {
            sb.append("X");
        }

        BTreeIndexFactory<String, String> indexFactory = new BTreeIndexFactory<String, String>();
        indexFactory.setKeyCodec(StringCodec.INSTANCE);
        indexFactory.setValueCodec(StringCodec.INSTANCE);

        Transaction tx = file.tx();
        SortedIndex<String, String> index = indexFactory.create(tx);
        tx.commit();
        tx.flush();

        // we update using the same key, which means we should be able to do this within the file size limit
        for (int i = 0; i < size; i++) {
            tx = file.tx();
            index = indexFactory.open(tx);
            index.put("foo", i + "-" + sb);
            tx.commit();
            tx.flush();
        }

        tx = file.tx();
        index = indexFactory.open(tx);
        System.out.println(index.get("foo"));
        tx.commit();

    }

}
