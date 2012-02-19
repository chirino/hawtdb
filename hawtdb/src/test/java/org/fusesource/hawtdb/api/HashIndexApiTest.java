package org.fusesource.hawtdb.api;

import org.junit.Test;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author Sergio Bossa
 */
public class HashIndexApiTest extends AbstractApiTest {

    @Override
    protected IndexFactory<String, String> getIndexFactory() {
        return new HashIndexFactory<String, String>();
    }
    
    static class Complex implements Serializable {
        public short a;
        public short b;
        public Complex(short a, short b) {
            this.a = a;
            this.b = b;
        }
        
        @Override
        public boolean equals(Object obj) {
            if(obj instanceof Complex) {
                Complex x = (Complex)obj;
                return a == x.a && b == x.b;       
            } else {
                return false;
            }
        }
        
        @Override
        public int hashCode() {
            return ((a & 0xFFFF) << 16) & b;
        }
        
        @Override
        public String toString() {
            return "(" + a + ", " + b + ")";
        }
    }
    
    @Test
    public void testUncomparableKey() throws IOException {
        File tmpFile = File.createTempFile("hawtdb", "test");

        PageFileFactory pageFactory = new PageFileFactory();
        pageFactory.setFile(tmpFile);
        pageFactory.open();

        PageFile page = pageFactory.getPageFile();

        IndexFactory<Complex, String> indexFactory = new HashIndexFactory<Complex, String>();

        Index<Complex, String> index = indexFactory.create(page);

        Complex c1 = new Complex((short)0, (short)0);
        index.put(c1, c1.toString());
        
        Complex c2 = new Complex((short)1, (short)1);
        index.put(c2, c2.toString());

        Assert.assertEquals(c1.toString(), index.get(c1));

        Assert.assertEquals(c2.toString(), index.get(c2));

        pageFactory.close();

        tmpFile.delete();
    }
}
