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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.fusesource.hawtdb.api.*;
import org.fusesource.hawtdb.api.PagedAccessor;
import org.fusesource.hawtbuf.Buffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TxPageFileTest {

    private TxPageFileFactory pff;
    private TxPageFile pf;

    protected TxPageFileFactory createConcurrentPageFileFactory() {
        TxPageFileFactory rc = new TxPageFileFactory();
        rc.setFile(new File("target/test-data/" + getClass().getName() + ".db"));
        return rc;
    }

    @Before
    public void setUp() throws Exception {
        pff = createConcurrentPageFileFactory();
        pff.getFile().delete();
        pff.open();
        pf = pff.getTxPageFile();
    }

    @After
    public void tearDown() throws Exception {
        pff.close();
    }

    protected void reload() throws IOException {
        pff.close();
        pff.open();
        pf = pff.getTxPageFile();
    }

    protected int store(Paged tx, String value) throws IOException {
        int pageId = tx.allocator().alloc(1);
        store(tx, pageId, value);
        return pageId;
    }

    protected void store(Paged tx, int page, String value) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream os = new DataOutputStream(baos);
            os.writeUTF(value);
            os.close();
            tx.write(page, new Buffer(baos.toByteArray()));
        } catch (IOException e) {
            throw new IOPagingException(e);
        }
    }

    protected String load(Paged paged, int page) {
        try {
            Buffer buffer = new Buffer(pff.getPageSize());
            paged.read(page, buffer);
            ByteArrayInputStream bais = new ByteArrayInputStream(buffer.data, buffer.offset, buffer.length);
            DataInputStream is = new DataInputStream(bais);
            return is.readUTF();
        } catch (IOException e) {
            throw new IOPagingException(e);
        }
    }

    private final class StringPagedAccessor implements PagedAccessor<String> {
        public String load(Paged paged, int page) {
            return TxPageFileTest.this.load(paged, page);
        }
        public List<Integer> store(Paged paged, int page, String value) {
            TxPageFileTest.this.store(paged, page, value);
            return Collections.emptyList();
        }
        public List<Integer> pagesLinked(Paged paged, int page) {
            return Collections.emptyList();
        }
    }

    @Test
    public void cacheAPI() throws IOException, ClassNotFoundException {

        // Setup some pages that will be getting updated.
        Transaction tx = pf.tx();
        StringPagedAccessor ENCODER = new StringPagedAccessor();
        tx.put(ENCODER, tx.allocator().alloc(1), "Hello");
        tx.put(ENCODER, tx.allocator().alloc(1), "World");
        tx.commit();

        reload();
        tx = pf.tx();

        assertEquals("Hello", tx.get(ENCODER, 0));
        assertEquals("World", tx.get(ENCODER, 1));

    }
    
    
    @Test
    public void cacheAPIConflictingUpdateFails() throws IOException, ClassNotFoundException {

        // Setup some pages that will be getting updated.
        Transaction tx1 = pf.tx();
        StringPagedAccessor ENCODER = new StringPagedAccessor();
        tx1.put(ENCODER, tx1.allocator().alloc(1), "Hello");
        tx1.put(ENCODER, tx1.allocator().alloc(1), "World");
        tx1.commit();

        tx1.put(ENCODER, 0, "Change 1");
        
        // Now commit a change to page 0
        Transaction tx2 = pf.tx();
        assertEquals("Hello", tx2.get(ENCODER, 0));  // We don't see tx1's change...
        tx2.put(ENCODER, 0, "Change 2");
        assertEquals("Change 2", tx2.get(ENCODER, 0)); // We can see our own change..
        tx2.commit();

        // Tx1 still does not see tx2's change...
        assertEquals("Change 1", tx1.get(ENCODER, 0));
        
        try {
            tx1.commit();
            fail("expected OptimisticUpdateException");
        } catch (OptimisticUpdateException expected) {
        }

    }

    @Test
    public void conflictingUpdateFails() throws IOException, ClassNotFoundException {

        // Setup some pages that will be getting updated.
        Transaction tx1 = pf.tx();
        assertEquals(0, store(tx1, "Hello"));
        assertEquals(1, store(tx1, "World"));
        tx1.commit();

        // Start a transaction that updates page 0
        tx1 = pf.tx();
        store(tx1, 0, "Change 1");

        // Now commit a change to page 0
        Transaction tx2 = pf.tx();
        assertEquals("Hello", load(tx2, 0)); // We don't see tx1's change...
        store(tx2, 0, "Change 2");
        assertEquals("Change 2", load(tx2, 0)); // We can see our own change..
        tx2.commit();

        // Tx1 still does not see tx2's change...
        assertEquals("Change 1", load(tx1, 0));

        try {
            tx1.commit();
            fail("expected OptimisticUpdateException");
        } catch (OptimisticUpdateException expected) {
        }

    }

    Paged getRawPageFile() {
        return ((HawtTxPageFile)pf).pageFile;
    }
    
    @Test
    public void pagesNotDirectlyUpdated() throws IOException, ClassNotFoundException {
        // New allocations get stored in the final positions.
        Transaction tx = pf.tx();
        assertEquals(0, store(tx, "Hello"));
        assertEquals(1, store(tx, "World"));

        // It should be on the page file already..
        assertEquals("Hello", load(getRawPageFile(), 0));
        assertEquals("World", load(getRawPageFile(), 1));
        tx.commit();

        // Apply the updates.
        pf.flush();
        ((HawtTxPageFile)pf).performBatches();

        // Should still be there..
        assertEquals("Hello", load(getRawPageFile(), 0));
        assertEquals("World", load(getRawPageFile(), 1));

        // Update the existing pages..
        store(tx, 0, "Good");
        store(tx, 1, "Bye");
        tx.commit();

        // A subsequent transaction can read the update.
        assertEquals("Good", load(tx, 0));
        assertEquals("Bye", load(tx, 1));
        tx.commit();

        // But the pages are should not be updated until the transaction gets
        // applied.
        assertEquals("Hello", load(getRawPageFile(), 0));
        assertEquals("World", load(getRawPageFile(), 1));

        // Apply them
        pf.flush();
        ((HawtTxPageFile)pf).performBatches();

        // We should see them now.
        assertEquals("Good", load(getRawPageFile(), 0));
        assertEquals("Bye", load(getRawPageFile(), 1));
    }

    @Test
    public void crudOperations() throws IOException, ClassNotFoundException {
        int COUNT = 10;

        ArrayList<Integer> allocations = new ArrayList<Integer>();
        HashSet<String> expected = new HashSet<String>();

        // Insert some data into the page file.
        Transaction tx = pf.tx();
        for (int i = 0; i < COUNT; i++) {

            int page = tx.allocator().alloc(1);
            // Since the file is empty.. allocations should occur sequentially
            assertEquals(i, page);

            allocations.add(page);
            String value = "page:" + i;
            store(tx, page, value);
            expected.add(value);
            tx.commit();
        }

        // Reload it.. .
        reload();
        tx = pf.tx();

        // Iterate it to make sure they are still there..
        HashSet<String> actual = new HashSet<String>();
        for (Integer page : allocations) {
            actual.add((String) load(tx, page));
        }
        assertEquals(expected, actual);

        // Remove the odd records..
        for (int i = 0; i < COUNT; i++) {
            if (i % 2 == 0) {
                break;
            }
            String t = "page:" + i;
            expected.remove(t);
        }
        for (Integer page : new ArrayList<Integer>(allocations)) {
            String t = (String) load(tx, page);
            if (!expected.contains(t)) {
                tx.free(page);
                allocations.remove(page);
            }
        }
        tx.commit();

        // Reload it...
        reload();
        tx = pf.tx();

        // Iterate it to make sure the even records are still there..
        actual.clear();
        for (Integer page : allocations) {
            String t = (String) load(tx, page);
            actual.add(t);
        }
        assertEquals(expected, actual);

        // Update the records...
        HashSet<String> t = expected;
        expected = new HashSet<String>();
        for (String s : t) {
            expected.add(s + ":updated");
        }
        for (Integer page : allocations) {
            String value = (String) load(tx, page);
            store(tx, page, value + ":updated");
        }
        tx.commit();

        // Reload it...
        reload();
        tx = pf.tx();

        // Iterate it to make sure the updated records are still there..
        actual.clear();
        for (Integer page : allocations) {
            String value = (String) load(tx, page);
            actual.add(value);
        }
        assertEquals(expected, actual);

    }

    @Test
    public void testExtentStreams() throws IOException {
        Transaction tx = pf.tx();
        ExtentOutputStream eos = new ExtentOutputStream(tx);
        DataOutputStream os = new DataOutputStream(eos);
        for (int i = 0; i < 10000; i++) {
            os.writeUTF("Test string:" + i);
        }
        os.close();
        int page = eos.getPage();
        tx.commit();

        // Reload the page file.
        reload();
        tx = pf.tx();

        ExtentInputStream eis = new ExtentInputStream(tx, page);
        DataInputStream is = new DataInputStream(eis);
        for (int i = 0; i < 10000; i++) {
            assertEquals("Test string:" + i, is.readUTF());
        }
        assertEquals(-1, is.read());
        is.close();
    }

    @Test
    public void testAddRollback() throws IOException, ClassNotFoundException {

        // Insert some data into the page file.
        Transaction tx = pf.tx();
        for (int i = 0; i < 10; i++) {
            String t = "page:" + i;
            int page1 = store(tx, t);
            tx.rollback();
            int page2 = store(tx, t);
            tx.rollback();

            // page allocation should get rollback so we should 
            // continually get the same page.
            assertEquals(page1, page2);
        }

    }
}
