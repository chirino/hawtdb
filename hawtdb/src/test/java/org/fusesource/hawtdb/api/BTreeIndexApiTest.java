package org.fusesource.hawtdb.api;

/**
 * @author Sergio Bossa
 */
public class BTreeIndexApiTest extends AbstractApiTest {

    @Override
    protected IndexFactory<String, String> getIndexFactory() {
        return new BTreeIndexFactory<String, String>();
    }
    
}
