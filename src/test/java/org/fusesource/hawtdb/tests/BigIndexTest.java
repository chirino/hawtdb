/*
 *  Copyright 2010 sergio.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */

package org.fusesource.hawtdb.tests;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Random;
import org.fusesource.hawtdb.api.BTreeIndexFactory;
import org.fusesource.hawtdb.api.PageFile;
import org.fusesource.hawtdb.api.PageFileFactory;
import org.fusesource.hawtdb.api.SortedIndex;
import org.junit.Test;

/**
 * @author Sergio Bossa
 */
public class BigIndexTest {

    @Test
    public void testBugOnBigIndex() throws IOException {
        File tmpFile = File.createTempFile("hawtdb", "test");

        PageFileFactory pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        PageFile page = pageFactory.getPageFile();

        BTreeIndexFactory<String, String> indexFactory = new BTreeIndexFactory<String, String>();
        indexFactory.setComparator(new Comparator<String>() {
            public int compare(String o1, String o2) {
                return o1.compareTo(o2) * -1;
            }
        });

        SortedIndex<String, String> index = indexFactory.create(page);

        int total = 10000;
        Random generator = new Random();
        for (int i = 0; i < total; i++) {
            index.put("" + generator.nextLong(), "");
        }

        pageFactory.close();
    }

}