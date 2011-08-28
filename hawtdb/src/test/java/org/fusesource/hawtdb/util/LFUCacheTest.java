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
package org.fusesource.hawtdb.util;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class LFUCacheTest {

    @Test
    public void testBasicOperations() {
        LFUCache<String, String> cache = new LFUCache<String, String>(100, 0.5f);

        cache.put("a", "a");
        assertEquals(1, cache.frequencyOf("a"));

        String v = cache.get("a");
        assertEquals("a", v);
        assertEquals(2, cache.frequencyOf("a"));

        assertNull(cache.get("b"));
        assertEquals(0, cache.frequencyOf("b"));

        assertEquals(1, cache.size());

        assertEquals("a", cache.remove("a"));
        assertNull(cache.get("a"));
        assertEquals(0, cache.frequencyOf("a"));

        assertEquals(0, cache.size());

        cache.put("a", "a");
        assertEquals("a", cache.get("a"));
        cache.put("a", "b");
        assertEquals("b", cache.get("a"));

        cache.clear();

        assertEquals(0, cache.size());
    }

    @Test
    public void testMaxFrequencyOverflow() {
        int max = 100;
        LFUCache<Integer, Integer> cache = new LFUCache<Integer, Integer>(max, 0.5f);

        cache.put(0, 0);
        for (int i = 0; i < max; i++) {
            assertEquals(Integer.valueOf(0), cache.get(0));
        }
    }

    @Test
    public void testElementsWithSameMaxFrequency() {
        int max = 2;
        LFUCache<Integer, Integer> cache = new LFUCache<Integer, Integer>(max, 0.5f);

        cache.put(0, 0);
        cache.put(1, 1);
        // Reach max frequency:
        cache.get(0);
        cache.get(1);
        // Overflow for 0 hence put it "higher" than 1.
        cache.get(0);
        // Put another element hence verify 1 has been evicted, not 0:
        cache.put(2, 2);
        assertEquals(Integer.valueOf(0), cache.get(0));
        assertEquals(Integer.valueOf(2), cache.get(2));
        assertEquals(2, cache.size());
    }

    @Test
    public void testPutUntilEvictionWithEqualFrequency() {
        int max = 100;
        LFUCache<String, String> cache = new LFUCache<String, String>(max, 0.5f);

        for (int i = 0; i < max; i++) {
            cache.put("" + i, "" + i);
        }
        assertEquals(max, cache.size());

        cache.put("" + max, "" + max);
        assertEquals(51, cache.size());
    }

    @Test
    public void testPutUntilEvictionWithDifferentFrequencies() {
        int max = 100;
        LFUCache<String, String> cache = new LFUCache<String, String>(max, 0.75f);

        for (int i = 0; i < max; i++) {
            cache.put("" + i, "" + i);
        }
        assertEquals(max, cache.size());

        for (int i = 0; i < max / 2; i++) {
            cache.get("" + i);
        }
        for (int i = 0; i < max / 4; i++) {
            cache.get("" + i);
        }

        cache.put("" + max, "" + max);

        for (int i = 0; i < max / 4; i++) {
            assertEquals("" + i, cache.get("" + i));
        }
        for (int i = max / 4; i < max; i++) {
            assertNull(cache.get("" + i));
        }
        assertEquals("" + max, cache.get("" + max));
        assertEquals(26, cache.size());
    }

}
