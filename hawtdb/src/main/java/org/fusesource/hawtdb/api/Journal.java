package org.fusesource.hawtdb.api;

import java.nio.ByteBuffer;

/**
 * Created by IntelliJ IDEA.
 * User: chirino
 * Date: 7/30/11
 * Time: 2:12 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Journal {

    public void append(long position, ByteBuffer buffer, Runnable onComplete);

}
