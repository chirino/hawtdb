/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.hawtdb.api;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Interface used to selectively visit the entries in a BTree.
 *
 * @param <Key>
 * @param <Value>
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface IndexVisitor<Key,Value> {

    /**
     * Do you want to visit the range of BTree entries between the first and and second key?
     *
     * @param first if null indicates the range of values before the second key.
     * @param second if null indicates the range of values after the first key.
     * @param comparator the Comparator configured for the index, may be null.
     *
     * @return true if you want to visit the values between the first and second key.
     */
    boolean isInterestedInKeysBetween(Key first, Key second, Comparator comparator);

    /**
     * The keys and values of an index node.
     *
     * @param keys
     * @param comparator the Comparator configured for the index, may be null.
     * @param values
     */
    void visit(List<Key> keys, List<Value> values, Comparator comparator);

    /**
     * @return true if the visitor has quenched it's thirst for more results
     */
    boolean isSatiated();

}