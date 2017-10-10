<<<<<<< HEAD
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
=======
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
>>>>>>> origin/0.10.2
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.tests;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
<<<<<<< HEAD
import org.apache.kafka.common.requests.IsolationLevel;
=======
>>>>>>> origin/0.10.2
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.test.TestUtils;

import java.io.File;
import java.util.Collections;
<<<<<<< HEAD
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
=======
import java.util.Properties;
>>>>>>> origin/0.10.2

public class BrokerCompatibilityTest {

    private static final String SOURCE_TOPIC = "brokerCompatibilitySourceTopic";
    private static final String SINK_TOPIC = "brokerCompatibilitySinkTopic";

<<<<<<< HEAD
    public static void main(final String[] args) throws Exception {
=======
    public static void main(String[] args) throws Exception {
>>>>>>> origin/0.10.2
        System.out.println("StreamsTest instance started");

        final String kafka = args.length > 0 ? args[0] : "localhost:9092";
        final String stateDirStr = args.length > 1 ? args[1] : TestUtils.tempDirectory().getAbsolutePath();
<<<<<<< HEAD
        final boolean eosEnabled = args.length > 2 ? Boolean.parseBoolean(args[2]) : false;
=======
>>>>>>> origin/0.10.2

        final File stateDir = new File(stateDirStr);
        stateDir.mkdir();

        final Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-system-test-broker-compatibility");
        streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        streamsProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
<<<<<<< HEAD
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        if (eosEnabled) {
            streamsProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        }
        final int timeout = 6000;
        streamsProperties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), timeout);
        streamsProperties.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG), timeout);
        streamsProperties.put(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout + 1);
=======
        streamsProperties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
>>>>>>> origin/0.10.2


        final KStreamBuilder builder = new KStreamBuilder();
        builder.stream(SOURCE_TOPIC).to(SINK_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder, streamsProperties);
<<<<<<< HEAD
        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread t, final Throwable e) {
                System.out.println("FATAL: An unexpected exception is encountered on thread " + t + ": " + e);

                streams.close(30, TimeUnit.SECONDS);
            }
        });
=======
>>>>>>> origin/0.10.2
        System.out.println("start Kafka Streams");
        streams.start();


        System.out.println("send data");
        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        producer.send(new ProducerRecord<>(SOURCE_TOPIC, "key", "value"));


        System.out.println("wait for result");
<<<<<<< HEAD
        loopUntilRecordReceived(kafka, eosEnabled);
=======
        loopUntilRecordReceived(kafka);
>>>>>>> origin/0.10.2


        System.out.println("close Kafka Streams");
        streams.close();
    }

<<<<<<< HEAD
    private static void loopUntilRecordReceived(final String kafka, final boolean eosEnabled) {
=======
    private static void loopUntilRecordReceived(final String kafka) {
>>>>>>> origin/0.10.2
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "broker-compatibility-consumer");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
<<<<<<< HEAD
        if (eosEnabled) {
            consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));
        }
=======
>>>>>>> origin/0.10.2

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(SINK_TOPIC));

        while (true) {
<<<<<<< HEAD
            final ConsumerRecords<String, String> records = consumer.poll(100);
            for (final ConsumerRecord<String, String> record : records) {
=======
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
>>>>>>> origin/0.10.2
                if (record.key().equals("key") && record.value().equals("value")) {
                    consumer.close();
                    return;
                }
            }
        }
    }

}
