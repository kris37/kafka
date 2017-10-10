/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 */
<<<<<<< HEAD:streams/src/main/java/org/apache/kafka/streams/state/internals/MergedSortedCacheKeyValueStoreIterator.java
class MergedSortedCacheKeyValueStoreIterator<K, V> extends AbstractMergedSortedCacheStoreIterator<K, Bytes, V, byte[]> {

    private final StateSerdes<K, V> serdes;

    MergedSortedCacheKeyValueStoreIterator(final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                           final KeyValueIterator<Bytes, byte[]> storeIterator,
                                           final StateSerdes<K, V> serdes) {
        super(cacheIterator, storeIterator);
        this.serdes = serdes;
=======
class MergedSortedCacheKeyValueBytesStoreIterator extends AbstractMergedSortedCacheStoreIterator<Bytes, Bytes, byte[], byte[]> {


    MergedSortedCacheKeyValueBytesStoreIterator(final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                                final KeyValueIterator<Bytes, byte[]> storeIterator) {
        super(cacheIterator, storeIterator);
    }

    @Override
    public KeyValue<Bytes, byte[]> deserializeStorePair(final KeyValue<Bytes, byte[]> pair) {
        return pair;
>>>>>>> apache/trunk:streams/src/main/java/org/apache/kafka/streams/state/internals/MergedSortedCacheKeyValueBytesStoreIterator.java
    }

    @Override
    Bytes deserializeCacheKey(final Bytes cacheKey) {
        return cacheKey;
    }

    @Override
    byte[] deserializeCacheValue(final LRUCacheEntry cacheEntry) {
        return cacheEntry.value;
    }

    @Override
<<<<<<< HEAD:streams/src/main/java/org/apache/kafka/streams/state/internals/MergedSortedCacheKeyValueStoreIterator.java
    V deserializeCacheValue(final LRUCacheEntry cacheEntry) {
        return serdes.valueFrom(cacheEntry.value);
    }

    @Override
    public K deserializeStoreKey(final Bytes key) {
        return serdes.keyFrom(key.get());
=======
    public Bytes deserializeStoreKey(final Bytes key) {
        return key;
>>>>>>> apache/trunk:streams/src/main/java/org/apache/kafka/streams/state/internals/MergedSortedCacheKeyValueBytesStoreIterator.java
    }

    @Override
    public int compare(final Bytes cacheKey, final Bytes storeKey) {
        return cacheKey.compareTo(storeKey);
    }
}
