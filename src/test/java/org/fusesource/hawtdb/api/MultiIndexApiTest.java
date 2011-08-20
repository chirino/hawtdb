package org.fusesource.hawtdb.api;

import java.io.File;
import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class MultiIndexApiTest {

    @Test
    public void testManuallyCreateAndOpenIndex() throws IOException {
        File tmpFile = File.createTempFile("hawtdb", "test");

        // Create:

        PageFileFactory pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        PageFile page = pageFactory.getPageFile();

        MultiIndexFactory multiIndexFactory = new MultiIndexFactory(page);
        IndexFactory<String, String> indexFactory1 = new BTreeIndexFactory<String, String>();
        IndexFactory<String, String> indexFactory2 = new HashIndexFactory<String, String>();

        Index<String, String> index1 = multiIndexFactory.create("1", indexFactory1);
        Index<String, String> index2 = multiIndexFactory.create("2", indexFactory2);

        assertEquals(2, multiIndexFactory.indexes().size());
        assertTrue(multiIndexFactory.indexes().contains("1"));
        assertTrue(multiIndexFactory.indexes().contains("2"));

        index1.put("1", "a");
        index2.put("a", "b");

        pageFactory.close();

        // Open:

        pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        page = pageFactory.getPageFile();

        multiIndexFactory = new MultiIndexFactory(page);
        indexFactory1 = new BTreeIndexFactory<String, String>();
        indexFactory2 = new HashIndexFactory<String, String>();

        index1 = multiIndexFactory.open("1", indexFactory1);
        index2 = multiIndexFactory.open("2", indexFactory2);

        assertEquals("b", index2.get(index1.get("1")));

        pageFactory.close();

        //

        tmpFile.delete();
    }

    @Test
    public void testOpenOrCreateIndex() throws IOException {
        File tmpFile = File.createTempFile("hawtdb", "test");

        // Create:

        PageFileFactory pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        PageFile page = pageFactory.getPageFile();

        MultiIndexFactory multiIndexFactory = new MultiIndexFactory(page);
        IndexFactory<String, String> indexFactory1 = new BTreeIndexFactory<String, String>();
        IndexFactory<String, String> indexFactory2 = new HashIndexFactory<String, String>();

        Index<String, String> index1 = multiIndexFactory.openOrCreate("1", indexFactory1);
        Index<String, String> index2 = multiIndexFactory.openOrCreate("2", indexFactory2);

        assertEquals(2, multiIndexFactory.indexes().size());
        assertTrue(multiIndexFactory.indexes().contains("1"));
        assertTrue(multiIndexFactory.indexes().contains("2"));

        index1.put("1", "a");
        index2.put("a", "b");

        pageFactory.close();

        // Open:

        pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        page = pageFactory.getPageFile();

        multiIndexFactory = new MultiIndexFactory(page);
        indexFactory1 = new BTreeIndexFactory<String, String>();
        indexFactory2 = new HashIndexFactory<String, String>();

        index1 = multiIndexFactory.openOrCreate("1", indexFactory1);
        index2 = multiIndexFactory.openOrCreate("2", indexFactory2);

        assertEquals("b", index2.get(index1.get("1")));

        pageFactory.close();

        //

        tmpFile.delete();
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotCreateIndexWithSameName() throws IOException {
        File tmpFile = File.createTempFile("hawtdb", "test");

        PageFileFactory pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        PageFile page = pageFactory.getPageFile();

        try {
            MultiIndexFactory multiIndexFactory = new MultiIndexFactory(page);
            IndexFactory<String, String> indexFactory1 = new BTreeIndexFactory<String, String>();
            IndexFactory<String, String> indexFactory2 = new HashIndexFactory<String, String>();
            Index<String, String> index1 = multiIndexFactory.create("1", indexFactory1);
            Index<String, String> index2 = multiIndexFactory.create("1", indexFactory2);
            fail("Exception was expected!");
        } finally {
            pageFactory.close();
            tmpFile.delete();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotOpenNotExistentIndex() throws IOException {
        File tmpFile = File.createTempFile("hawtdb", "test");

        PageFileFactory pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        PageFile page = pageFactory.getPageFile();

        try {
            MultiIndexFactory multiIndexFactory = new MultiIndexFactory(page);
            IndexFactory<String, String> indexFactory1 = new BTreeIndexFactory<String, String>();
            Index<String, String> index1 = multiIndexFactory.open("1", indexFactory1);
            fail("Exception was expected!");
        } finally {
            pageFactory.close();
            tmpFile.delete();
        }
    }

}
