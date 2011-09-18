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
package org.fusesource.hawtdb.api;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Provides Key/Value storage and retrieval.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface SortedIndex<Key,Value> extends Index<Key,Value>, Iterable<Map.Entry<Key, Value>> {

    /**
     * @return
     * @throws IOException
     */
    public Iterator<Map.Entry<Key,Value>> iterator();

    /**
     * @return
     * @throws IOException
     */
    public Iterator<Map.Entry<Key,Value>> iterator(Predicate<Key> predicate);

    /**
     * 
     * @param initialKey
     * @return
     */
    public Iterator<Map.Entry<Key, Value>> iterator(Key initialKey);


    /**
     * Traverses the visitor over the stored entries in this index.  The visitor can control
     * which keys and values are visited.
     *
     * @param visitor
     */
    public void visit(IndexVisitor<Key, Value> visitor);

    /**
     *
     * @return the first key/value pair in the index or null if empty.
     */
    public Map.Entry<Key, Value> getFirst();

    /**
     * @return the last key/value pair in the index or null if empty.
     */
    public Map.Entry<Key, Value> getLast();


}
