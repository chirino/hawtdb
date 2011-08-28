package org.fusesource.hawtdb.api;

/**
 * @author Sergio Bossa
 */
public class HashIndexApiTest extends AbstractApiTest {

    @Override
    protected IndexFactory<String, String> getIndexFactory() {
        return new HashIndexFactory<String, String>();
    }
    
}
