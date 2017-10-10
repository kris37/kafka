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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SessionKeySerdeTest {

    final private String topic = "topic";
    final private String key = "key";
    final private long startTime = 50L;
    final private long endTime = 100L;
    final private Window window = new SessionWindow(startTime, endTime);
    final private Windowed<String> windowedKey = new Windowed<>(key, window);
    final private Serde<String> serde = Serdes.String();
    final private SessionKeySerde<String> sessionKeySerde = new SessionKeySerde<>(serde);

    @Test
    public void shouldSerializeDeserialize() throws Exception {
        final byte[] bytes = sessionKeySerde.serializer().serialize(topic, windowedKey);
        final Windowed<String> result = sessionKeySerde.deserializer().deserialize(topic, bytes);
        assertEquals(windowedKey, result);
    }

    @Test
    public void shouldSerializeNullToNull() throws Exception {
        assertNull(sessionKeySerde.serializer().serialize(topic, null));
    }

    @Test
    public void shouldDeSerializeEmtpyByteArrayToNull() throws Exception {
        assertNull(sessionKeySerde.deserializer().deserialize(topic, new byte[0]));
    }

    @Test
    public void shouldDeSerializeNullToNull() throws Exception {
        assertNull(sessionKeySerde.deserializer().deserialize(topic, null));
    }

    @Test
    public void shouldConvertToBinaryAndBack() throws Exception {
<<<<<<< HEAD
        final Bytes serialized = SessionKeySerde.toBinary(windowedKey, serde.serializer(), "dummy");
        final Windowed<String> result = SessionKeySerde.from(serialized.get(), Serdes.String().deserializer(), "dummy");
        assertEquals(windowedKey, result);
=======
        final Windowed<String> key = new Windowed<>("key", new SessionWindow(10, 20));
        final Bytes serialized = SessionKeySerde.toBinary(key, Serdes.String().serializer(), "topic");
        final Windowed<String> result = SessionKeySerde.from(serialized.get(), Serdes.String().deserializer(), "topic");
        assertEquals(key, result);
>>>>>>> origin/0.10.2
    }

    @Test
    public void shouldExtractEndTimeFromBinary() throws Exception {
<<<<<<< HEAD
        final Bytes serialized = SessionKeySerde.toBinary(windowedKey, serde.serializer(), "dummy");
        assertEquals(endTime, SessionKeySerde.extractEnd(serialized.get()));
=======
        final Windowed<String> key = new Windowed<>("key", new SessionWindow(10, 100));
        final Bytes serialized = SessionKeySerde.toBinary(key, Serdes.String().serializer(), "topic");
        assertEquals(100, SessionKeySerde.extractEnd(serialized.get()));
>>>>>>> origin/0.10.2
    }

    @Test
    public void shouldExtractStartTimeFromBinary() throws Exception {
<<<<<<< HEAD
        final Bytes serialized = SessionKeySerde.toBinary(windowedKey, serde.serializer(), "dummy");
        assertEquals(startTime, SessionKeySerde.extractStart(serialized.get()));
    }

    @Test
    public void shouldExtractWindowFromBindary() throws Exception {
        final Bytes serialized = SessionKeySerde.toBinary(windowedKey, serde.serializer(), "dummy");
        assertEquals(window, SessionKeySerde.extractWindow(serialized.get()));
=======
        final Windowed<String> key = new Windowed<>("key", new SessionWindow(50, 100));
        final Bytes serialized = SessionKeySerde.toBinary(key, Serdes.String().serializer(), "topic");
        assertEquals(50, SessionKeySerde.extractStart(serialized.get()));
>>>>>>> origin/0.10.2
    }

    @Test
    public void shouldExtractKeyBytesFromBinary() throws Exception {
<<<<<<< HEAD
        final Bytes serialized = SessionKeySerde.toBinary(windowedKey, serde.serializer(), "dummy");
        assertArrayEquals(key.getBytes(), SessionKeySerde.extractKeyBytes(serialized.get()));
    }

    @Test
    public void shouldExtractKeyFromBinary() throws Exception {
        final Bytes serialized = SessionKeySerde.toBinary(windowedKey, serde.serializer(), "dummy");
        assertEquals(windowedKey, SessionKeySerde.from(serialized.get(), serde.deserializer(), "dummy"));
    }

    @Test
    public void shouldExtractBytesKeyFromBinary() throws Exception {
        final Bytes bytesKey = Bytes.wrap(key.getBytes());
        final Windowed<Bytes> windowedBytesKey = new Windowed<>(bytesKey, window);
        final Bytes serialized = SessionKeySerde.bytesToBinary(windowedBytesKey);
        assertEquals(windowedBytesKey, SessionKeySerde.fromBytes(serialized));
    }
=======
        final Windowed<String> key = new Windowed<>("blah", new SessionWindow(50, 100));
        final Bytes serialized = SessionKeySerde.toBinary(key, Serdes.String().serializer(), "topic");
        assertArrayEquals("blah".getBytes(), SessionKeySerde.extractKeyBytes(serialized.get()));
    }

    @Test
    public void shouldExtractBytesKeyFromBinary() throws Exception {
        final Bytes bytesKey = Bytes.wrap("key".getBytes());
        final Windowed<Bytes> windowedBytesKey = new Windowed<>(bytesKey, new SessionWindow(0, 10));
        final Bytes serialized = SessionKeySerde.bytesToBinary(windowedBytesKey);
        assertEquals(windowedBytesKey, SessionKeySerde.fromBytes(serialized));
    }

>>>>>>> origin/0.10.2
}