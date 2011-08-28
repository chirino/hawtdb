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
package org.fusesource.hawtdb.internal.index;

import org.fusesource.hawtdb.api.HashIndexFactory;
import org.fusesource.hawtdb.api.Index;
import org.fusesource.hawtbuf.codec.LongCodec;
import org.fusesource.hawtbuf.codec.StringCodec;


/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class HashIndexTest extends IndexTestSupport {

    @Override
    protected Index<String, Long> createIndex(int page) {
        HashIndexFactory<String,Long> factory = new HashIndexFactory<String,Long>();
        factory.setKeyCodec(StringCodec.INSTANCE);
        factory.setValueCodec(LongCodec.INSTANCE);
        // Configure a large fixed capacity in order to avoid too much resizings which would saturate memory:
        factory.setFixedCapacity(8192);
        //
        factory.setDeferredEncoding(false);
        if( page==-1 ) {
            return factory.create(tx);
        } else {
            return factory.open(tx, page);
        }
    }
}
