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

import org.fusesource.hawtdb.api.Predicate;

import java.util.*;
import java.util.Map.Entry;

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final class BTreePredicateIterator<Key, Value> implements Iterator<Entry<Key, Value>> {

    private final BTreeIndex<Key, Value> index;
    private Predicate<Key> predicate;
    private final Stack<Data<Key, Value>> stack = new Stack<Data<Key, Value>>();


    private class Data<Key, Value> {
        private BTreeNode<Key, Value> node;
        private int pos;

        public Data(BTreeNode<Key, Value> node) {
            this.node = node;
        }
    }

    private Entry<Key, Value> nextEntry;

    BTreePredicateIterator(BTreeIndex<Key, Value> index, BTreeNode<Key, Value> root, Predicate<Key> predicate) {
        this.index = index;
        this.predicate = predicate;
        stack.push(new Data<Key, Value>(root));
    }

    private void findNextEntry() {
        while ( nextEntry==null && !stack.isEmpty() ) {
            final Data<Key, Value> current = stack.peek();
            final BTreeNode<Key, Value> node = current.node;
            final BTreeNode.Data<Key, Value> data = node.data;

            if (node.isBranch()) {
                if (current.pos < data.children.length) {
                    Key key1 = null;
                    if (current.pos != 0) {
                        key1 = data.keys[current.pos - 1];
                    }
                    Key key2 = null;
                    if (current.pos != data.children.length - 1) {
                        key2 = data.keys[current.pos];
                    }
                    if (predicate.isInterestedInKeysBetween(key1, key2, index.getComparator())) {
                        stack.push( new Data<Key, Value>(node.getChild(index, current.pos)) );
                    }
                    current.pos++;
                } else {
                    stack.pop();
                }
            } else {
                if (current.pos < data.keys.length) {
                    if( predicate.isInterestedInKey(data.keys[current.pos], index.getComparator()) ) {
                        nextEntry = new MapEntry<Key, Value>(data.keys[current.pos], data.values[current.pos]);
                    }
                    current.pos++;
                } else {
                    stack.pop();
                }
            }
        }
    }

    public boolean hasNext() {
        findNextEntry();
        return nextEntry != null;
    }

    public Entry<Key, Value> next() {
        findNextEntry();
        if (nextEntry != null) {
            Entry<Key, Value> lastEntry = nextEntry;
            nextEntry = null;
            return lastEntry;
        } else {
            throw new NoSuchElementException();
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }


    
}