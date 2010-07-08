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

import java.util.Comparator;
import java.util.List;

/**
 * A predicate is used to narrow down the keys that an application is interested in 
 * accessing.
 *
 * You can implement custom predicate implementations by implementing the Predicate interface or
 * you can you some of the predefined predicate classes.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @param <Key>
 */
public interface Predicate<Key> {
    
    /**
     * 
     * @param first the first key in the range or null if unknown
     * @param second the last key in the range or null if unknown
     * @param comparator the Comparator configured for the index, may be null.
     *
     * @return true if the predicate is interested in keys in the range.
     */
    boolean isInterestedInKeysBetween(Key first, Key second, Comparator comparator);
    
    /**
     * @param key
     * @param comparator the Comparator configured for the index, may be null.
     *
     * @return true if the predicate is interested in the key
     */
    boolean isInterestedInKey(Key key, Comparator comparator);
    
}