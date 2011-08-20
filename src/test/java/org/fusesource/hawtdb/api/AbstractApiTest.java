package org.fusesource.hawtdb.api;

import java.io.File;
import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public abstract class AbstractApiTest {

    @Test
    public void testManuallyCreateAndOpenIndex() throws IOException {
        File tmpFile = File.createTempFile("hawtdb", "test");

        // Create:

        PageFileFactory pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        PageFile page = pageFactory.getPageFile();

        IndexFactory<String, String> indexFactory = getIndexFactory();

        Index<String, String> index = indexFactory.create(page);

        index.put("1", "1");

        pageFactory.close();

        // Open:

        pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        page = pageFactory.getPageFile();

        index = indexFactory.open(page);

        assertEquals("1", index.get("1"));

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

        IndexFactory<String, String> indexFactory = getIndexFactory();

        Index<String, String> index = indexFactory.openOrCreate(page);

        assertFalse(index.containsKey("1"));
        
        index.put("1", "1");

        pageFactory.close();

        // Open:

        pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        page = pageFactory.getPageFile();

        index = indexFactory.openOrCreate(page);

        assertEquals("1", index.get("1"));

        pageFactory.close();

        //

        tmpFile.delete();
    }
    
    protected abstract IndexFactory<String, String> getIndexFactory();
}