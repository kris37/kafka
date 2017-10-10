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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
<<<<<<< HEAD
import org.apache.kafka.streams.kstream.Window;
=======
>>>>>>> origin/0.10.2
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.List;


<<<<<<< HEAD
class CachingSessionStore<K, AGG> extends WrappedStateStore.AbstractStateStore implements SessionStore<K, AGG>, CachedStateStore<Windowed<K>, AGG> {
=======
class CachingSessionStore<K, AGG> extends WrappedStateStore.AbstractWrappedStateStore implements SessionStore<K, AGG>, CachedStateStore<Windowed<K>, AGG> {
>>>>>>> origin/0.10.2

    private final SessionStore<Bytes, byte[]> bytesStore;
    private final SessionKeySchema keySchema;
    private final Serde<K> keySerde;
    private final Serde<AGG> aggSerde;
<<<<<<< HEAD
    private final SegmentedCacheFunction cacheFunction;
    private String cacheName;
=======
    private InternalProcessorContext context;
    private String cacheName;
    private StateSerdes<K, AGG> serdes;
>>>>>>> origin/0.10.2
    private ThreadCache cache;
    private StateSerdes<K, AGG> serdes;
    private InternalProcessorContext context;
    private CacheFlushListener<Windowed<K>, AGG> flushListener;
    private String topic;

    CachingSessionStore(final SessionStore<Bytes, byte[]> bytesStore,
                        final Serde<K> keySerde,
<<<<<<< HEAD
                        final Serde<AGG> aggSerde,
                        final long segmentInterval) {
=======
                        final Serde<AGG> aggSerde) {
>>>>>>> origin/0.10.2
        super(bytesStore);
        this.bytesStore = bytesStore;
        this.keySerde = keySerde;
        this.aggSerde = aggSerde;
        this.keySchema = new SessionKeySchema();
        this.cacheFunction = new SegmentedCacheFunction(keySchema, segmentInterval);
    }

    public void init(final ProcessorContext context, final StateStore root) {
        topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), root.name());
        bytesStore.init(context, root);
        initInternal((InternalProcessorContext) context);
    }

    @SuppressWarnings("unchecked")
    private void initInternal(final InternalProcessorContext context) {
        this.context = context;

        keySchema.init(topic);
        serdes = new StateSerdes<>(
            topic,
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            aggSerde == null ? (Serde<AGG>) context.valueSerde() : aggSerde);


        cacheName = context.taskId() + "-" + bytesStore.name();
        cache = context.getCache();
        cache.addDirtyEntryFlushListener(cacheName, new ThreadCache.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<ThreadCache.DirtyEntry> entries) {
                for (ThreadCache.DirtyEntry entry : entries) {
                    putAndMaybeForward(entry, context);
                }
            }
        });
    }

    public KeyValueIterator<Windowed<K>, AGG> findSessions(final K key,
                                                           final long earliestSessionEndTime,
                                                           final long latestSessionStartTime) {
        validateStoreOpen();
        final Bytes binarySessionId = Bytes.wrap(serdes.rawKey(key));
<<<<<<< HEAD

        final Bytes cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(binarySessionId, earliestSessionEndTime));
        final Bytes cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRangeFixedSize(binarySessionId, latestSessionStartTime));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(cacheName, cacheKeyFrom, cacheKeyTo);

        final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = bytesStore.findSessions(
            binarySessionId, earliestSessionEndTime, latestSessionStartTime
        );
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(binarySessionId,
                                                                             binarySessionId,
                                                                             earliestSessionEndTime,
                                                                             latestSessionStartTime);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
        return new MergedSortedCacheSessionStoreIterator<>(filteredCacheIterator, storeIterator, serdes, cacheFunction);
=======
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(cacheName,
                                                                                  keySchema.lowerRange(binarySessionId,
                                                                                                       earliestSessionEndTime).get(),
                                                                                  keySchema.upperRange(binarySessionId, latestSessionStartTime).get());
        final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = bytesStore.findSessions(binarySessionId, earliestSessionEndTime, latestSessionStartTime);
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(binarySessionId,
                                                                             earliestSessionEndTime,
                                                                             latestSessionStartTime);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(cacheIterator, hasNextCondition);
        return new MergedSortedCacheSessionStoreIterator<>(filteredCacheIterator, storeIterator, serdes);
>>>>>>> origin/0.10.2
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> findSessions(K keyFrom,
                                                           K keyTo,
                                                           long earliestSessionEndTime,
                                                           long latestSessionStartTime) {
        validateStoreOpen();
        final Bytes binarySessionIdFrom = Bytes.wrap(serdes.rawKey(keyFrom));
        final Bytes binarySessionIdTo = Bytes.wrap(serdes.rawKey(keyTo));

        final Bytes cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRange(binarySessionIdFrom, earliestSessionEndTime));
        final Bytes cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRange(binarySessionIdTo, latestSessionStartTime));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(cacheName, cacheKeyFrom, cacheKeyTo);

        final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = bytesStore.findSessions(
            binarySessionIdFrom, binarySessionIdTo, earliestSessionEndTime, latestSessionStartTime
        );
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(binarySessionIdFrom, binarySessionIdTo,
                                                                             earliestSessionEndTime,
                                                                             latestSessionStartTime);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
        return new MergedSortedCacheSessionStoreIterator<>(filteredCacheIterator, storeIterator, serdes, cacheFunction);
    }

    @Override
    public void remove(final Windowed<K> sessionKey) {
        validateStoreOpen();
        put(sessionKey, null);
    }

    @Override
    public void put(final Windowed<K> key, AGG value) {
        validateStoreOpen();
        final Bytes binaryKey = SessionKeySerde.toBinary(key, serdes.keySerializer(), topic);
        final LRUCacheEntry entry = new LRUCacheEntry(serdes.rawValue(value), true, context.offset(),
                                                      key.window().end(), context.partition(), context.topic());
<<<<<<< HEAD
        cache.put(cacheName, cacheFunction.cacheKey(binaryKey), entry);
=======
        cache.put(cacheName, binaryKey.get(), entry);
>>>>>>> origin/0.10.2
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> fetch(final K key) {
        return findSessions(key, 0, Long.MAX_VALUE);
    }

<<<<<<< HEAD
    @Override
    public KeyValueIterator<Windowed<K>, AGG> fetch(K from, K to) {
        return findSessions(from, to, 0, Long.MAX_VALUE);
    }

=======
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context, final StateStore root) {
        topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), root.name());
        bytesStore.init(context, root);
        initInternal((InternalProcessorContext) context);
    }

    @SuppressWarnings("unchecked")
    private void initInternal(final InternalProcessorContext context) {
        this.context = context;

        keySchema.init(topic);
        this.serdes = new StateSerdes<>(topic,
                                        keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                                        aggSerde == null ? (Serde<AGG>) context.valueSerde() : aggSerde);


        this.cacheName = context.taskId() + "-" + bytesStore.name();
        this.cache = this.context.getCache();
        cache.addDirtyEntryFlushListener(cacheName, new ThreadCache.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<ThreadCache.DirtyEntry> entries) {
                for (ThreadCache.DirtyEntry entry : entries) {
                    putAndMaybeForward(entry, context);
                }
            }
        });
>>>>>>> origin/0.10.2


    private void putAndMaybeForward(final ThreadCache.DirtyEntry entry, final InternalProcessorContext context) {
        final Bytes binaryKey = cacheFunction.key(entry.key());
        final RecordContext current = context.recordContext();
        context.setRecordContext(entry.recordContext());
        try {
            final Windowed<K> key = SessionKeySerde.from(binaryKey.get(), serdes.keyDeserializer(), topic);
<<<<<<< HEAD
            final Bytes rawKey = Bytes.wrap(serdes.rawKey(key.key()));
=======
>>>>>>> origin/0.10.2
            if (flushListener != null) {
                final AGG newValue = serdes.valueFrom(entry.newValue());
                final AGG oldValue = fetchPrevious(rawKey, key.window());
                if (!(newValue == null && oldValue == null)) {
                    flushListener.apply(key, newValue, oldValue);
                }
            }
<<<<<<< HEAD
            bytesStore.put(new Windowed<>(rawKey, key.window()), entry.newValue());
=======
            bytesStore.put(new Windowed<>(Bytes.wrap(serdes.rawKey(key.key())), key.window()), entry.newValue());
>>>>>>> origin/0.10.2
        } finally {
            context.setRecordContext(current);
        }
    }

<<<<<<< HEAD
    private AGG fetchPrevious(final Bytes rawKey, final Window window) {
        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = bytesStore
                .findSessions(rawKey, window.start(), window.end())) {
=======
    private AGG fetchPrevious(final Bytes key) {
        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = bytesStore.fetch(key)) {
>>>>>>> origin/0.10.2
            if (!iterator.hasNext()) {
                return null;
            }
            return serdes.valueFrom(iterator.next().value);
        }
    }

    public void flush() {
        cache.flush(cacheName);
        bytesStore.flush();
    }

    public void close() {
        flush();
        cache.close(cacheName);
        bytesStore.close();
<<<<<<< HEAD
=======
        cache.close(cacheName);
>>>>>>> origin/0.10.2
    }

    public void setFlushListener(CacheFlushListener<Windowed<K>, AGG> flushListener) {
        this.flushListener = flushListener;
    }

}
