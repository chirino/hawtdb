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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.fusesource.hawtbuf.codec.LongCodec;
import org.fusesource.hawtbuf.codec.StringCodec;
import org.fusesource.hawtdb.api.BTreeIndexFactory;
import org.fusesource.hawtdb.api.Index;
import org.fusesource.hawtdb.api.IndexVisitor;
import org.fusesource.hawtbuf.Buffer;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class BTreeIndexTest extends IndexTestSupport {

    private NumberFormat nf;

    @Before
    public void setUp() throws Exception {
        nf = NumberFormat.getIntegerInstance();
        nf.setMinimumIntegerDigits(6);
        nf.setGroupingUsed(false);
    }
    
    @Override
    protected Index<String, Long> createIndex(int page) {
        BTreeIndexFactory<String,Long> factory = new BTreeIndexFactory<String,Long>();
        factory.setKeyCodec(StringCodec.INSTANCE);
        factory.setValueCodec(LongCodec.INSTANCE);
        factory.setDeferredEncoding(false);
        if( page==-1 ) {
            return factory.create(tx);
        } else {
            return factory.open(tx, page);
        }
    }
    
    @Test
    public void lotsOfInsertsWithTxsThatStayOpen() throws Exception {
        createPageFileAndIndex((short) (200));
        BTreeIndex<String, Long> index = ((BTreeIndex<String, Long>)this.index);
        for (int i = 0; i < 1000*5; i++) {
            // Do a read on a new tx to trigger a tx which is not closed.
            pf.tx().read(0, new Buffer(1024));

            index.put(key(i), (long)i);
            tx.commit();
            
            assertEquals(new Long(i), index.get(key(i)));
            tx.commit();
        }
    } 
        
    @Test
    public void lotsOfInserts() throws Exception {
        createPageFileAndIndex((short) (200));
        BTreeIndex<String, Long> index = ((BTreeIndex<String, Long>)this.index);
        for (int i = 0; i < 1000*5; i++) {
            index.put(key(i), (long)i);
            tx.commit();
            
            assertEquals(new Long(i), index.get(key(i)));
            tx.commit();
        }
    }     
    
    /**
     * Yeah, the current implementation does NOT try to balance the tree.  Here is 
     * a test case showing that it gets out of balance.  
     * 
     * @throws Exception
     */
    public void treeBalancing() throws Exception {
        createPageFileAndIndex((short) 100);

        BTreeIndex<String, Long> index = ((BTreeIndex<String, Long>)this.index);
        
        doInsert(50);
        
        int minLeafDepth = index.getMinLeafDepth();
        int maxLeafDepth = index.getMaxLeafDepth();
        assertTrue("Tree is balanced", maxLeafDepth-minLeafDepth <= 1);

        // Remove some of the data
        doRemove(16);
        minLeafDepth = index.getMinLeafDepth();
        maxLeafDepth = index.getMaxLeafDepth();

        System.out.println( "min:"+minLeafDepth );
        System.out.println( "max:"+maxLeafDepth );
        index.printStructure(new PrintWriter(System.out));

        assertTrue("Tree is balanced", maxLeafDepth-minLeafDepth <= 1);
        
        tx.commit();
    }
    
    @Test
    public void testPruning() throws Exception {
        createPageFileAndIndex((short)100);

        BTreeIndex<String,Long> index = ((BTreeIndex<String,Long>)this.index);

        int minLeafDepth = index.getMinLeafDepth();
        int maxLeafDepth = index.getMaxLeafDepth();
        assertEquals(1, minLeafDepth);
        assertEquals(1, maxLeafDepth);
        
        doInsert(1000);
        
        reloadAll();
        
        index = ((BTreeIndex<String,Long>)this.index);
        minLeafDepth = index.getMinLeafDepth();
        maxLeafDepth = index.getMaxLeafDepth();
        assertTrue("Depth of tree grew", minLeafDepth > 1);
        assertTrue("Depth of tree grew", maxLeafDepth > 1);

        // Remove the data.
        doRemove(1000);
        minLeafDepth = index.getMinLeafDepth();
        maxLeafDepth = index.getMaxLeafDepth();

        assertEquals(1, minLeafDepth);
        assertEquals(1, maxLeafDepth);
    }

    @Test
    public void testIteration() throws Exception {
        createPageFileAndIndex((short)100);
        
        BTreeIndex<String,Long> index = ((BTreeIndex<String,Long>)this.index);
          
        // Insert in reverse order..
        doInsertReverse(1000);
        
        reloadIndex();
        tx.commit();

        // BTree should iterate it in sorted order.
        int counter=0;
        for (Map.Entry<String,Long> entry : index) {
            assertEquals(key(counter),entry.getKey());
            assertEquals(counter,(long)entry.getValue());
            counter++;
        }
    }
    
    
    @Test
    public void testVisitor() throws Exception {
        createPageFileAndIndex((short)100);
        BTreeIndex<String,Long> index = ((BTreeIndex<String,Long>)this.index);
          
        // Insert in reverse order..
        doInsert(1000);
        
        reloadIndex();
        tx.commit();

        // BTree should iterate it in sorted order.
        
        index.visit(new IndexVisitor<String, Long>(){
            public boolean isInterestedInKeysBetween(String first, String second, Comparator comparator) {
                return true;
            }
            public void visit(List<String> strings, List<Long> longs, Comparator comparator) {
            }
            public boolean isSatiated() {
                return false;
            }
        });

    }
    
    void doInsertReverse(int count) throws Exception {
        for (int i = count-1; i >= 0; i--) {
            index.put(key(i), (long)i);
            tx.commit();
        }
    }
    /**
     * Overriding so that this generates keys that are the worst case for the BTree. Keys that
     * always insert to the end of the BTree.  
     */
    @Override
    protected String key(int i) {
        return "key:"+nf.format(i);
    }
       
}
