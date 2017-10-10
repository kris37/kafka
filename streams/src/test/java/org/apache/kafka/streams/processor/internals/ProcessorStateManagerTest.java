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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
<<<<<<< HEAD
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.MockTime;
=======
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
>>>>>>> origin/0.10.2
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockChangelogReader;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProcessorStateManagerTest {

<<<<<<< HEAD
=======
    public static class MockRestoreConsumer extends MockConsumer<byte[], byte[]> {
        private final Serializer<Integer> serializer = new IntegerSerializer();

        private TopicPartition assignedPartition = null;
        private long seekOffset = -1L;
        private boolean seekToBeginingCalled = false;
        private boolean seekToEndCalled = false;
        private long endOffset = 0L;
        private long currentOffset = 0L;

        private ArrayList<ConsumerRecord<byte[], byte[]>> recordBuffer = new ArrayList<>();

        MockRestoreConsumer() {
            super(OffsetResetStrategy.EARLIEST);

            reset();
        }

        // reset this mock restore consumer for a state store registration
        public void reset() {
            assignedPartition = null;
            seekOffset = -1L;
            seekToBeginingCalled = false;
            seekToEndCalled = false;
            endOffset = 0L;
            recordBuffer.clear();
        }

        // buffer a record (we cannot use addRecord because we need to add records before assigning a partition)
        public void bufferRecord(ConsumerRecord<Integer, Integer> record) {
            recordBuffer.add(
                new ConsumerRecord<>(record.topic(), record.partition(), record.offset(), 0L,
                    TimestampType.CREATE_TIME, 0L, 0, 0,
                    serializer.serialize(record.topic(), record.key()),
                    serializer.serialize(record.topic(), record.value())));
            endOffset = record.offset();

            super.updateEndOffsets(Collections.singletonMap(assignedPartition, endOffset));
        }

        @Override
        public synchronized void assign(Collection<TopicPartition> partitions) {
            int numPartitions = partitions.size();
            if (numPartitions > 1)
                throw new IllegalArgumentException("RestoreConsumer: more than one partition specified");

            if (numPartitions == 1) {
                if (assignedPartition != null)
                    throw new IllegalStateException("RestoreConsumer: partition already assigned");
                assignedPartition = partitions.iterator().next();

                // set the beginning offset to 0
                // NOTE: this is users responsible to set the initial lEO.
                super.updateBeginningOffsets(Collections.singletonMap(assignedPartition, 0L));
            }

            super.assign(partitions);
        }

        @Override
        public ConsumerRecords<byte[], byte[]> poll(long timeout) {
            // add buffered records to MockConsumer
            for (ConsumerRecord<byte[], byte[]> record : recordBuffer) {
                super.addRecord(record);
            }
            recordBuffer.clear();

            ConsumerRecords<byte[], byte[]> records = super.poll(timeout);

            // set the current offset
            Iterable<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(assignedPartition);
            for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                currentOffset = record.offset();
            }

            return records;
        }

        @Override
        public synchronized long position(TopicPartition partition) {
            if (!partition.equals(assignedPartition))
                throw new IllegalStateException("RestoreConsumer: unassigned partition");

            return currentOffset;
        }

        @Override
        public synchronized void seek(TopicPartition partition, long offset) {
            if (offset < 0)
                throw new IllegalArgumentException("RestoreConsumer: offset should not be negative");

            if (seekOffset >= 0)
                throw new IllegalStateException("RestoreConsumer: offset already seeked");

            seekOffset = offset;
            currentOffset = offset;
            super.seek(partition, offset);
        }

        @Override
        public synchronized void seekToBeginning(Collection<TopicPartition> partitions) {
            if (partitions.size() != 1)
                throw new IllegalStateException("RestoreConsumer: other than one partition specified");

            for (TopicPartition partition : partitions) {
                if (!partition.equals(assignedPartition))
                    throw new IllegalStateException("RestoreConsumer: seek-to-end not on the assigned partition");
            }

            seekToBeginingCalled = true;
            currentOffset = 0L;
        }

        @Override
        public synchronized void seekToEnd(Collection<TopicPartition> partitions) {
            if (partitions.size() != 1)
                throw new IllegalStateException("RestoreConsumer: other than one partition specified");

            for (TopicPartition partition : partitions) {
                if (!partition.equals(assignedPartition))
                    throw new IllegalStateException("RestoreConsumer: seek-to-end not on the assigned partition");
            }

            seekToEndCalled = true;
            currentOffset = endOffset;
        }
    }

>>>>>>> origin/0.10.2
    private final Set<TopicPartition> noPartitions = Collections.emptySet();
    private final String applicationId = "test-application";
    private final String persistentStoreName = "persistentStore";
    private final String nonPersistentStoreName = "nonPersistentStore";
    private final String persistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, persistentStoreName);
    private final String nonPersistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, nonPersistentStoreName);
    private final MockStateStoreSupplier.MockStateStore persistentStore = new MockStateStoreSupplier.MockStateStore(persistentStoreName, true);
    private final MockStateStoreSupplier.MockStateStore nonPersistentStore = new MockStateStoreSupplier.MockStateStore(nonPersistentStoreName, false);
    private final TopicPartition persistentStorePartition = new TopicPartition(persistentStoreTopicName, 1);
    private final String storeName = "mockStateStore";
    private final String changelogTopic = ProcessorStateManager.storeChangelogTopic(applicationId, storeName);
    private final TopicPartition changelogTopicPartition = new TopicPartition(changelogTopic, 0);
    private final TaskId taskId = new TaskId(0, 1);
<<<<<<< HEAD
    private final MockChangelogReader changelogReader = new MockChangelogReader();
    private final MockStateStoreSupplier.MockStateStore mockStateStore = new MockStateStoreSupplier.MockStateStore(storeName, true);
    private File baseDir;
    private File checkpointFile;
    private OffsetCheckpoint checkpoint;
    private StateDirectory stateDirectory;

=======
    private final MockRestoreConsumer restoreConsumer = new MockRestoreConsumer();
    private final MockStateStoreSupplier.MockStateStore mockStateStore = new MockStateStoreSupplier.MockStateStore(storeName, true);
    private File baseDir;
    private File checkpointFile;
    private StateDirectory stateDirectory;
    private OffsetCheckpoint checkpoint;
>>>>>>> origin/0.10.2

    @Before
    public void setup() {
        baseDir = TestUtils.tempDirectory();
<<<<<<< HEAD
        stateDirectory = new StateDirectory(applicationId, baseDir.getPath(), new MockTime());
        checkpointFile = new File(stateDirectory.directoryForTask(taskId), ProcessorStateManager.CHECKPOINT_FILE_NAME);
        checkpoint = new OffsetCheckpoint(checkpointFile);
=======
        stateDirectory = new StateDirectory(applicationId, baseDir.getPath());
        checkpointFile = new File(stateDirectory.directoryForTask(taskId), ProcessorStateManager.CHECKPOINT_FILE_NAME);
        checkpoint = new OffsetCheckpoint(checkpointFile);
        restoreConsumer.updatePartitions(persistentStoreTopicName, Utils.mkList(
                new PartitionInfo(persistentStoreTopicName, 1, Node.noNode(), new Node[0], new Node[0])
        ));
        restoreConsumer.updatePartitions(nonPersistentStoreTopicName, Utils.mkList(
                new PartitionInfo(nonPersistentStoreTopicName, 1, Node.noNode(), new Node[0], new Node[0])
        ));
>>>>>>> origin/0.10.2
    }

    @After
    public void cleanup() throws IOException {
        Utils.delete(baseDir);
    }

<<<<<<< HEAD
=======
    @Test(expected = StreamsException.class)
    public void testNoTopic() throws IOException {
        MockStateStoreSupplier.MockStateStore mockStateStore = new MockStateStoreSupplier.MockStateStore(nonPersistentStoreName, false);

        ProcessorStateManager stateMgr = new ProcessorStateManager(new TaskId(0, 1), noPartitions, new MockRestoreConsumer(), false, stateDirectory, new HashMap<String, String>() {
            {
                put(nonPersistentStoreName, nonPersistentStoreName);
            }
        });
        try {
            stateMgr.register(mockStateStore, true, mockStateStore.stateRestoreCallback);
        } finally {
            stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
        }
    }

>>>>>>> origin/0.10.2
    @Test
    public void testRegisterPersistentStore() throws IOException {
        final TaskId taskId = new TaskId(0, 2);

        final MockStateStoreSupplier.MockStateStore persistentStore
            = new MockStateStoreSupplier.MockStateStore("persistentStore", true); // persistent store
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            new HashMap<String, String>() {
                {
                    put(persistentStoreName, persistentStoreTopicName);
                    put(nonPersistentStoreName, nonPersistentStoreName);
                }
            },
            changelogReader,
            false);

<<<<<<< HEAD
=======
        MockRestoreConsumer restoreConsumer = new MockRestoreConsumer();

        restoreConsumer.updatePartitions(persistentStoreTopicName, Utils.mkList(
                new PartitionInfo(persistentStoreTopicName, 1, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo(persistentStoreTopicName, 2, Node.noNode(), new Node[0], new Node[0])
        ));

        TopicPartition partition = new TopicPartition(persistentStoreTopicName, 2);
        restoreConsumer.updateEndOffsets(Collections.singletonMap(partition, 13L));

        MockStateStoreSupplier.MockStateStore persistentStore = new MockStateStoreSupplier.MockStateStore("persistentStore", true); // persistent store

        ProcessorStateManager stateMgr = new ProcessorStateManager(taskId, noPartitions, restoreConsumer, false, stateDirectory, new HashMap<String, String>() {
            {
                put(persistentStoreName, persistentStoreTopicName);
                put(nonPersistentStoreName, nonPersistentStoreName);
            }
        });
>>>>>>> origin/0.10.2
        try {
            stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);
            assertTrue(changelogReader.wasRegistered(new TopicPartition(persistentStoreTopicName, 2)));
        } finally {
            stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
        }
    }

    @Test
    public void testRegisterNonPersistentStore() throws IOException {
<<<<<<< HEAD
        final MockStateStoreSupplier.MockStateStore nonPersistentStore
            = new MockStateStoreSupplier.MockStateStore(nonPersistentStoreName, false); // non persistent store
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            new TaskId(0, 2),
            noPartitions,
            false,
            stateDirectory,
            new HashMap<String, String>() {
                {
                    put(persistentStoreName, persistentStoreTopicName);
                    put(nonPersistentStoreName, nonPersistentStoreTopicName);
                }
            },
            changelogReader,
            false);

        try {
=======
        long lastCheckpointedOffset = 10L;

        OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(baseDir, ProcessorStateManager.CHECKPOINT_FILE_NAME));
        checkpoint.write(Collections.singletonMap(new TopicPartition(persistentStoreTopicName, 2), lastCheckpointedOffset));

        restoreConsumer.updatePartitions(nonPersistentStoreTopicName, Utils.mkList(
                new PartitionInfo(nonPersistentStoreTopicName, 1, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo(nonPersistentStoreTopicName, 2, Node.noNode(), new Node[0], new Node[0])
        ));

        TopicPartition partition = new TopicPartition(persistentStoreTopicName, 2);
        restoreConsumer.updateEndOffsets(Collections.singletonMap(partition, 13L));

        ProcessorStateManager stateMgr = new ProcessorStateManager(new TaskId(0, 2), noPartitions, restoreConsumer, false, stateDirectory, new HashMap<String, String>() {
            {
                put(persistentStoreName, persistentStoreTopicName);
                put(nonPersistentStoreName, nonPersistentStoreTopicName);
            }
        });
        try {
            restoreConsumer.reset();

            ArrayList<Integer> expectedKeys = new ArrayList<>();
            long offset;
            for (int i = 1; i <= 3; i++) {
                offset = (long) (i + 100);
                int key = i;
                expectedKeys.add(i);
                restoreConsumer.bufferRecord(
                        new ConsumerRecord<>(nonPersistentStoreTopicName, 2, 0L, offset, TimestampType.CREATE_TIME, 0L, 0, 0, key, 0)
                );
            }

>>>>>>> origin/0.10.2
            stateMgr.register(nonPersistentStore, true, nonPersistentStore.stateRestoreCallback);
            assertTrue(changelogReader.wasRegistered(new TopicPartition(nonPersistentStoreTopicName, 2)));
        } finally {
            stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
        }
    }

    @Test
    public void testChangeLogOffsets() throws IOException {
        final TaskId taskId = new TaskId(0, 0);
<<<<<<< HEAD
        final long lastCheckpointedOffset = 10L;
        final String storeName1 = "store1";
        final String storeName2 = "store2";
        final String storeName3 = "store3";
=======
        final OffsetCheckpoint offsetCheckpoint = new OffsetCheckpoint(
                new File(stateDirectory.directoryForTask(taskId), ProcessorStateManager.CHECKPOINT_FILE_NAME));
        long lastCheckpointedOffset = 10L;
        String storeName1 = "store1";
        String storeName2 = "store2";
        String storeName3 = "store3";
>>>>>>> origin/0.10.2

        final String storeTopicName1 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName1);
        final String storeTopicName2 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName2);
        final String storeTopicName3 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName3);

        final Map<String, String> storeToChangelogTopic = new HashMap<>();
        storeToChangelogTopic.put(storeName1, storeTopicName1);
        storeToChangelogTopic.put(storeName2, storeTopicName2);
        storeToChangelogTopic.put(storeName3, storeTopicName3);

<<<<<<< HEAD
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(stateDirectory.directoryForTask(taskId), ProcessorStateManager.CHECKPOINT_FILE_NAME));
        checkpoint.write(Collections.singletonMap(new TopicPartition(storeTopicName1, 0), lastCheckpointedOffset));

        final TopicPartition partition1 = new TopicPartition(storeTopicName1, 0);
        final TopicPartition partition2 = new TopicPartition(storeTopicName2, 0);
        final TopicPartition partition3 = new TopicPartition(storeTopicName3, 1);
=======
        offsetCheckpoint.write(Collections.singletonMap(new TopicPartition(storeTopicName1, 0), lastCheckpointedOffset));

        restoreConsumer.updatePartitions(storeTopicName1, Utils.mkList(
                new PartitionInfo(storeTopicName1, 0, Node.noNode(), new Node[0], new Node[0])
        ));
        restoreConsumer.updatePartitions(storeTopicName2, Utils.mkList(
                new PartitionInfo(storeTopicName2, 0, Node.noNode(), new Node[0], new Node[0])
        ));
        restoreConsumer.updatePartitions(storeTopicName3, Utils.mkList(
                new PartitionInfo(storeTopicName3, 0, Node.noNode(), new Node[0], new Node[0]),
                new PartitionInfo(storeTopicName3, 1, Node.noNode(), new Node[0], new Node[0])
        ));

        TopicPartition partition1 = new TopicPartition(storeTopicName1, 0);
        TopicPartition partition2 = new TopicPartition(storeTopicName2, 0);
        TopicPartition partition3 = new TopicPartition(storeTopicName3, 1);
>>>>>>> origin/0.10.2

        final MockStateStoreSupplier.MockStateStore store1 = new MockStateStoreSupplier.MockStateStore(storeName1, true);
        final MockStateStoreSupplier.MockStateStore store2 = new MockStateStoreSupplier.MockStateStore(storeName2, true);
        final MockStateStoreSupplier.MockStateStore store3 = new MockStateStoreSupplier.MockStateStore(storeName3, true);

        // if there is a source partition, inherit the partition id
        final Set<TopicPartition> sourcePartitions = Utils.mkSet(new TopicPartition(storeTopicName3, 1));

        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            sourcePartitions,
            true, // standby
            stateDirectory,
            storeToChangelogTopic,
            changelogReader,
            false);

<<<<<<< HEAD
=======
        ProcessorStateManager stateMgr = new ProcessorStateManager(taskId, sourcePartitions, restoreConsumer, true, stateDirectory, storeToChangelogTopic); // standby
>>>>>>> origin/0.10.2
        try {
            stateMgr.register(store1, true, store1.stateRestoreCallback);
            stateMgr.register(store2, true, store2.stateRestoreCallback);
            stateMgr.register(store3, true, store3.stateRestoreCallback);

<<<<<<< HEAD
            final Map<TopicPartition, Long> changeLogOffsets = stateMgr.checkpointed();
=======
            Map<TopicPartition, Long> changeLogOffsets = stateMgr.checkpointed();
>>>>>>> origin/0.10.2

            assertEquals(3, changeLogOffsets.size());
            assertTrue(changeLogOffsets.containsKey(partition1));
            assertTrue(changeLogOffsets.containsKey(partition2));
            assertTrue(changeLogOffsets.containsKey(partition3));
            assertEquals(lastCheckpointedOffset, (long) changeLogOffsets.get(partition1));
            assertEquals(-1L, (long) changeLogOffsets.get(partition2));
            assertEquals(-1L, (long) changeLogOffsets.get(partition3));

        } finally {
            stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
        }
    }

    @Test
    public void testGetStore() throws IOException {
<<<<<<< HEAD
        final MockStateStoreSupplier.MockStateStore mockStateStore = new MockStateStoreSupplier.MockStateStore(nonPersistentStoreName, false);
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            new TaskId(0, 1),
            noPartitions,
            false,
            stateDirectory,
            Collections.<String, String>emptyMap(),
            changelogReader,
            false);
=======
        final ProcessorStateManager stateMgr = new ProcessorStateManager(new TaskId(0, 1), noPartitions, restoreConsumer, false, stateDirectory, Collections.<String, String>emptyMap());
>>>>>>> origin/0.10.2
        try {
            stateMgr.register(nonPersistentStore, true, nonPersistentStore.stateRestoreCallback);

            assertNull(stateMgr.getStore("noSuchStore"));
            assertEquals(nonPersistentStore, stateMgr.getStore(nonPersistentStoreName));

        } finally {
            stateMgr.close(Collections.<TopicPartition, Long>emptyMap());
        }
    }

    @Test
    public void testFlushAndClose() throws IOException {
<<<<<<< HEAD
=======
        // write an empty checkpoint file
>>>>>>> origin/0.10.2
        checkpoint.write(Collections.<TopicPartition, Long>emptyMap());

        // set up ack'ed offsets
        final HashMap<TopicPartition, Long> ackedOffsets = new HashMap<>();
        ackedOffsets.put(new TopicPartition(persistentStoreTopicName, 1), 123L);
        ackedOffsets.put(new TopicPartition(nonPersistentStoreTopicName, 1), 456L);
        ackedOffsets.put(new TopicPartition(ProcessorStateManager.storeChangelogTopic(applicationId, "otherTopic"), 1), 789L);

<<<<<<< HEAD
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            new HashMap<String, String>() {
                {
                    put(persistentStoreName, persistentStoreTopicName);
                    put(nonPersistentStoreName, nonPersistentStoreTopicName);
                }
            },
            changelogReader,
            false);
=======
        ProcessorStateManager stateMgr = new ProcessorStateManager(taskId, noPartitions, restoreConsumer, false, stateDirectory, new HashMap<String, String>() {
            {
                put(persistentStoreName, persistentStoreTopicName);
                put(nonPersistentStoreName, nonPersistentStoreTopicName);
            }
        });
>>>>>>> origin/0.10.2
        try {
            // make sure the checkpoint file isn't deleted
            assertTrue(checkpointFile.exists());

            stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);
            stateMgr.register(nonPersistentStore, true, nonPersistentStore.stateRestoreCallback);
        } finally {
            // close the state manager with the ack'ed offsets
            stateMgr.flush();
            stateMgr.close(ackedOffsets);
        }
        // make sure all stores are closed, and the checkpoint file is written.
        assertTrue(persistentStore.flushed);
        assertTrue(persistentStore.closed);
        assertTrue(nonPersistentStore.flushed);
        assertTrue(nonPersistentStore.closed);
        assertTrue(checkpointFile.exists());

        // the checkpoint file should contain an offset from the persistent store only.
        final Map<TopicPartition, Long> checkpointedOffsets = checkpoint.read();
        assertEquals(1, checkpointedOffsets.size());
        assertEquals(new Long(124), checkpointedOffsets.get(new TopicPartition(persistentStoreTopicName, 1)));
    }

    @Test
    public void shouldRegisterStoreWithoutLoggingEnabledAndNotBackedByATopic() throws Exception {
<<<<<<< HEAD
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            new TaskId(0, 1),
            noPartitions,
            false,
            stateDirectory,
            Collections.<String, String>emptyMap(),
            changelogReader,
            false);
=======
        ProcessorStateManager stateMgr = new ProcessorStateManager(new TaskId(0, 1), noPartitions, new MockRestoreConsumer(), false, stateDirectory, Collections.<String, String>emptyMap());
>>>>>>> origin/0.10.2
        stateMgr.register(nonPersistentStore, false, nonPersistentStore.stateRestoreCallback);
        assertNotNull(stateMgr.getStore(nonPersistentStoreName));
    }


    @Test
    public void shouldNotChangeOffsetsIfAckedOffsetsIsNull() throws Exception {
        final Map<TopicPartition, Long> offsets = Collections.singletonMap(persistentStorePartition, 99L);
        checkpoint.write(offsets);
<<<<<<< HEAD

        final MockStateStoreSupplier.MockStateStore persistentStore = new MockStateStoreSupplier.MockStateStore(persistentStoreName, true);
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            Collections.<String, String>emptyMap(),
            changelogReader,
            false);
        stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);
        stateMgr.close(null);
        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(offsets));
    }

    @Test
    public void shouldWriteCheckpointForPersistentLogEnabledStore() throws Exception {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            Collections.singletonMap(persistentStore.name(), persistentStoreTopicName),
            changelogReader,
            false);
        stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);


        stateMgr.checkpoint(Collections.singletonMap(persistentStorePartition, 10L));
        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(Collections.singletonMap(persistentStorePartition, 11L)));
    }

    @Test
    public void shouldWriteCheckpointForStandbyReplica() throws Exception {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            true, // standby
            stateDirectory,
            Collections.singletonMap(persistentStore.name(), persistentStoreTopicName),
            changelogReader,
            false);

        stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);
        final byte[] bytes = Serdes.Integer().serializer().serialize("", 10);
        stateMgr.updateStandbyStates(persistentStorePartition,
                                     Collections.singletonList(
                                             new ConsumerRecord<>(persistentStorePartition.topic(),
                                                                  persistentStorePartition.partition(),
                                                                  888L,
                                                                  bytes,
                                                                  bytes)));

        stateMgr.checkpoint(Collections.<TopicPartition, Long>emptyMap());

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(Collections.singletonMap(persistentStorePartition, 889L)));

    }

    @Test
    public void shouldNotWriteCheckpointForNonPersistent() throws Exception {
        final TopicPartition topicPartition = new TopicPartition(nonPersistentStoreTopicName, 1);

        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            true, // standby
            stateDirectory,
            Collections.singletonMap(nonPersistentStoreName, nonPersistentStoreTopicName),
            changelogReader,
            false);

        stateMgr.register(nonPersistentStore, true, nonPersistentStore.stateRestoreCallback);
        stateMgr.checkpoint(Collections.singletonMap(topicPartition, 876L));

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(Collections.<TopicPartition, Long>emptyMap()));
    }

    @Test
    public void shouldNotWriteCheckpointForStoresWithoutChangelogTopic() throws Exception {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            true, // standby
            stateDirectory,
            Collections.<String, String>emptyMap(),
            changelogReader,
            false);
=======

        final ProcessorStateManager stateMgr = new ProcessorStateManager(taskId,
                                                                         noPartitions,
                                                                         restoreConsumer,
                                                                         false,
                                                                         stateDirectory,
                                                                         Collections.<String, String>emptyMap());

        restoreConsumer.reset();
        stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);
        stateMgr.close(null);
        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(offsets));
    }

    @Test
    public void shouldWriteCheckpointForPersistentLogEnabledStore() throws Exception {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(taskId,
                                                                         noPartitions,
                                                                         restoreConsumer,
                                                                         false,
                                                                         stateDirectory,
                                                                         Collections.singletonMap(persistentStore.name(),
                                                                                                  persistentStoreTopicName));
        restoreConsumer.reset();
        stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);

        stateMgr.checkpoint(Collections.singletonMap(persistentStorePartition, 10L));
        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(Collections.singletonMap(persistentStorePartition, 11L)));
    }

    @Test
    public void shouldWriteCheckpointForStandbyReplica() throws Exception {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(taskId,
                                                                         noPartitions,
                                                                         restoreConsumer,
                                                                         true,
                                                                         stateDirectory,
                                                                         Collections.singletonMap(persistentStore.name(),
                                                                                                  persistentStoreTopicName));
>>>>>>> origin/0.10.2

        stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);
<<<<<<< HEAD

        stateMgr.checkpoint(Collections.singletonMap(persistentStorePartition, 987L));

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(Collections.<TopicPartition, Long>emptyMap()));
    }

    @Test
    public void shouldThrowLockExceptionIfFailedToLockStateDirectory() throws Exception {
        final File taskDirectory = stateDirectory.directoryForTask(taskId);
        final FileChannel channel = FileChannel.open(new File(taskDirectory,
                                                              StateDirectory.LOCK_FILE_NAME).toPath(),
                                                     StandardOpenOption.CREATE,
                                                     StandardOpenOption.WRITE);
        // lock the task directory
        final FileLock lock = channel.lock();

        try {
            new ProcessorStateManager(
                taskId,
                noPartitions,
                false,
                stateDirectory,
                Collections.<String, String>emptyMap(),
                changelogReader,
                false);
            fail("Should have thrown LockException");
        } catch (final LockException e) {
           // pass
        } finally {
            lock.release();
            channel.close();
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfStoreNameIsSameAsCheckpointFileName() throws Exception {
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            Collections.<String, String>emptyMap(),
            changelogReader,
            false);

        try {
            stateManager.register(new MockStateStoreSupplier.MockStateStore(ProcessorStateManager.CHECKPOINT_FILE_NAME, true), true, null);
            fail("should have thrown illegal argument exception when store name same as checkpoint file");
        } catch (final IllegalArgumentException e) {
            //pass
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionOnRegisterWhenStoreHasAlreadyBeenRegistered() throws Exception {
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            Collections.<String, String>emptyMap(),
            changelogReader,
            false);
        stateManager.register(mockStateStore, false, null);

        try {
            stateManager.register(mockStateStore, false, null);
            fail("should have thrown illegal argument exception when store with same name already registered");
        } catch (final IllegalArgumentException e) {
            // pass
        }

    }

    @Test
    public void shouldThrowProcessorStateExceptionOnCloseIfStoreThrowsAnException() throws Exception {

        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Collections.singleton(changelogTopicPartition),
            false,
            stateDirectory,
            Collections.singletonMap(storeName, changelogTopic),
            changelogReader,
            false);

        final MockStateStoreSupplier.MockStateStore stateStore = new MockStateStoreSupplier.MockStateStore(storeName, true) {
            @Override
            public void close() {
                throw new RuntimeException("KABOOM!");
            }
        };
        stateManager.register(stateStore, false, stateStore.stateRestoreCallback);

        try {
            stateManager.close(Collections.<TopicPartition, Long>emptyMap());
            fail("Should throw ProcessorStateException if store close throws exception");
        } catch (final ProcessorStateException e) {
            // pass
        }
    }

    @Test
    public void shouldDeleteCheckpointFileOnCreationIfEosEnabled() throws Exception {
        checkpoint.write(Collections.<TopicPartition, Long>emptyMap());
        assertTrue(checkpointFile.exists());

        ProcessorStateManager stateManager = null;
        try {
            stateManager = new ProcessorStateManager(
                taskId,
                noPartitions,
                false,
                stateDirectory,
                Collections.<String, String>emptyMap(),
                changelogReader,
                true);

            assertFalse(checkpointFile.exists());
        } finally {
            if (stateManager != null) {
                stateManager.close(null);
            }
        }
=======
        final byte[] bytes = Serdes.Integer().serializer().serialize("", 10);
        stateMgr.updateStandbyStates(persistentStorePartition,
                                     Collections.singletonList(
                                             new ConsumerRecord<>(persistentStorePartition.topic(),
                                                                                persistentStorePartition.partition(),
                                                                                888L,
                                                                                bytes,
                                                                                bytes)));

        stateMgr.checkpoint(Collections.<TopicPartition, Long>emptyMap());

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(Collections.singletonMap(persistentStorePartition, 889L)));

    }

    @Test
    public void shouldNotWriteCheckpointForNonPersistent() throws Exception {
        final TopicPartition topicPartition = new TopicPartition(nonPersistentStoreTopicName, 1);

        restoreConsumer.updatePartitions(nonPersistentStoreTopicName, Utils.mkList(
                new PartitionInfo(nonPersistentStoreTopicName, 1, Node.noNode(), new Node[0], new Node[0])
        ));

        final ProcessorStateManager stateMgr = new ProcessorStateManager(taskId,
                                                                         noPartitions,
                                                                         restoreConsumer,
                                                                         true,
                                                                         stateDirectory,
                                                                         Collections.singletonMap(nonPersistentStoreName,
                                                                                                  nonPersistentStoreTopicName));

        restoreConsumer.reset();
        stateMgr.register(nonPersistentStore, true, nonPersistentStore.stateRestoreCallback);
        stateMgr.checkpoint(Collections.singletonMap(topicPartition, 876L));

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(Collections.<TopicPartition, Long>emptyMap()));
    }

    @Test
    public void shouldNotWriteCheckpointForStoresWithoutChangelogTopic() throws Exception {
        final ProcessorStateManager stateMgr = new ProcessorStateManager(taskId,
                                                                         noPartitions,
                                                                         restoreConsumer,
                                                                         true,
                                                                         stateDirectory,
                                                                         Collections.<String, String>emptyMap());

        stateMgr.register(persistentStore, true, persistentStore.stateRestoreCallback);

        stateMgr.checkpoint(Collections.singletonMap(persistentStorePartition, 987L));

        final Map<TopicPartition, Long> read = checkpoint.read();
        assertThat(read, equalTo(Collections.<TopicPartition, Long>emptyMap()));
>>>>>>> origin/0.10.2
    }

}
