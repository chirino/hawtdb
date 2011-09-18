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

import org.fusesource.hawtdb.api.Index;
import org.fusesource.hawtbuf.codec.LongCodec;
import org.fusesource.hawtbuf.codec.StringCodec;
import org.fusesource.hawtdb.api.BTreeIndexFactory;
import org.fusesource.hawtdb.api.Transaction;

/**
 * @author Sergio Bossa
 */
public class BTreeIndexConcurrencyTest extends ConcurrencyTestSupport {

    @Override
    protected Index<String, Long> createIndex() {
        BTreeIndexFactory<String, Long> factory = new BTreeIndexFactory<String, Long>();
        factory.setKeyCodec(StringCodec.INSTANCE);
        factory.setValueCodec(LongCodec.INSTANCE);
        //
        Transaction tx = pageFile.tx();
        Index<String, Long> index = factory.create(tx);
        tx.commit();
        return index;
    }

    @Override
    protected Index<String, Long> openIndex(Transaction tx) {
        BTreeIndexFactory<String, Long> factory = new BTreeIndexFactory<String, Long>();
        factory.setKeyCodec(StringCodec.INSTANCE);
        factory.setValueCodec(LongCodec.INSTANCE);
        //
        Index<String, Long> index = factory.open(tx);
        return index;
    }

}
