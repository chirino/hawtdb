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

import org.fusesource.hawtbuf.codec.FixedBufferCodec;
import org.fusesource.hawtbuf.codec.LongCodec;
import org.fusesource.hawtdb.api.BTreeIndexFactory;
import org.fusesource.hawtdb.api.Index;
import org.fusesource.hawtdb.api.Transaction;
import org.fusesource.hawtbuf.Buffer;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class BTreeIndexBenchmark extends IndexBenchmark {

    public BTreeIndexBenchmark() {
        this.benchmark.setSamples(10);
    }
    
    protected Index<Long, Buffer> createIndex(Transaction tx) {
        BTreeIndexFactory<Long, Buffer> factory = new BTreeIndexFactory<Long, Buffer>();
        factory.setKeyCodec(LongCodec.INSTANCE);
        factory.setValueCodec(new FixedBufferCodec(DATA.length));
        return factory.create(tx);
    }

   
}
