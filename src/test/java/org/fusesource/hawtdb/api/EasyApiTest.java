package org.fusesource.hawtdb.api;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class EasyApiTest {

    @Test
    public void testEasyApi() throws IOException {
        File tmpFile = File.createTempFile("hawtdb", "test");

        // Create:

        PageFileFactory pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        PageFile page = pageFactory.getPageFile();

        BTreeIndexFactory<String, String> factory = new BTreeIndexFactory<String, String>();
        factory.setComparator(new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2) * -1;
            }
        });

        SortedIndex<String, String> index = factory.create(page);

        index.put("3", "");
        index.put("4", "");
        index.put("2", "");
        index.put("8", "");

        pageFactory.close();

        // Open:

        pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        page = pageFactory.getPageFile();

        factory = new BTreeIndexFactory<String, String>();
        factory.setComparator(new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2) * -1;
            }
        });

        index = factory.open(page);

        Iterator<Entry<String, String>> it = index.iterator("4");
        assertEquals("4", it.next().getKey());
        assertEquals("3", it.next().getKey());

        pageFactory.close();

        //

        tmpFile.delete();
    }
}