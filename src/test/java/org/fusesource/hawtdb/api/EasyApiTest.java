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
    public void testSingleIndex() throws IOException {
        File tmpFile = File.createTempFile("hawtdb", "test");

        // Create:

        PageFileFactory pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        PageFile page = pageFactory.getPageFile();

        BTreeIndexFactory<String, String> indexFactory = new BTreeIndexFactory<String, String>();
        indexFactory.setComparator(new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2) * -1;
            }
        });

        SortedIndex<String, String> index = indexFactory.create(page);

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

        indexFactory = new BTreeIndexFactory<String, String>();
        indexFactory.setComparator(new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2) * -1;
            }
        });

        index = indexFactory.open(page);

        Iterator<Entry<String, String>> it = index.iterator("4");
        assertEquals("4", it.next().getKey());
        assertEquals("3", it.next().getKey());

        pageFactory.close();

        //

        tmpFile.delete();
    }

    @Test
    public void testMultipleIndexes() throws IOException {
        File tmpFile = File.createTempFile("hawtdb", "test");

        // Create:

        PageFileFactory pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        PageFile page = pageFactory.getPageFile();

        BTreeIndexFactory<String, String> indexFactory = new BTreeIndexFactory<String, String>();
        indexFactory.setComparator(new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2) * -1;
            }
        });

        SortedIndex<String, String> index1 = indexFactory.create(page);
        int indexNumber1 = index1.getIndexNumber();
        index1.put("3", "");
        index1.put("4", "");
        SortedIndex<String, String> index2 = indexFactory.create(page);
        int indexNumber2 = index2.getIndexNumber();
        index2.put("2", "");
        index2.put("8", "");

        pageFactory.close();

        // Open:

        pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        page = pageFactory.getPageFile();

        indexFactory = new BTreeIndexFactory<String, String>();
        indexFactory.setComparator(new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2) * -1;
            }
        });

        index1 = indexFactory.open(page, indexNumber1);
        index2 = indexFactory.open(page, indexNumber2);

        Iterator<Entry<String, String>> it1 = index1.iterator("4");
        assertEquals("4", it1.next().getKey());
        assertEquals("3", it1.next().getKey());
        Iterator<Entry<String, String>> it2 = index2.iterator("8");
        assertEquals("8", it2.next().getKey());
        assertEquals("2", it2.next().getKey());

        pageFactory.close();

        //

        tmpFile.delete();
    }
}