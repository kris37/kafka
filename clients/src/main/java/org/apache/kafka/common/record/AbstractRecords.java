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
package org.apache.kafka.common.record;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractRecords implements Records {

    private final Iterable<Record> records = new Iterable<Record>() {
        @Override
        public Iterator<Record> iterator() {
<<<<<<< HEAD
            return recordsIterator();
=======
            return new Iterator<Record>() {
                private final Iterator<? extends LogEntry> deepEntries = deepEntries(BufferSupplier.NO_CACHING).iterator();
                @Override
                public boolean hasNext() {
                    return deepEntries.hasNext();
                }
                @Override
                public Record next() {
                    return deepEntries.next().record();
                }
                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Removal not supported");
                }
            };
>>>>>>> origin/0.10.2
        }
    };

    @Override
    public boolean hasMatchingMagic(byte magic) {
        for (RecordBatch batch : batches())
            if (batch.magic() != magic)
                return false;
        return true;
    }

    @Override
    public boolean hasCompatibleMagic(byte magic) {
        for (RecordBatch batch : batches())
            if (batch.magic() > magic)
                return false;
        return true;
    }

    /**
     * Down convert batches to the provided message format version. The first offset parameter is only relevant in the
     * conversion from uncompressed v2 or higher to v1 or lower. The reason is that uncompressed records in v0 and v1
     * are not batched (put another way, each batch always has 1 record).
     *
     * If a client requests records in v1 format starting from the middle of an uncompressed batch in v2 format, we
     * need to drop records from the batch during the conversion. Some versions of librdkafka rely on this for
     * correctness.
     */
<<<<<<< HEAD
    protected MemoryRecords downConvert(Iterable<? extends RecordBatch> batches, byte toMagic, long firstOffset) {
        // maintain the batch along with the decompressed records to avoid the need to decompress again
        List<RecordBatchAndRecords> recordBatchAndRecordsList = new ArrayList<>();
        int totalSizeEstimate = 0;

        for (RecordBatch batch : batches) {
            if (toMagic < RecordBatch.MAGIC_VALUE_V2 && batch.isControlBatch())
                continue;

            if (batch.magic() <= toMagic) {
                totalSizeEstimate += batch.sizeInBytes();
                recordBatchAndRecordsList.add(new RecordBatchAndRecords(batch, null, null));
            } else {
                List<Record> records = new ArrayList<>();
                for (Record record : batch) {
                    // See the method javadoc for an explanation
                    if (toMagic > RecordBatch.MAGIC_VALUE_V1 || batch.isCompressed() || record.offset() >= firstOffset)
                        records.add(record);
                }
                if (records.isEmpty())
                    continue;
                final long baseOffset;
                if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 && toMagic >= RecordBatch.MAGIC_VALUE_V2)
                    baseOffset = batch.baseOffset();
                else
                    baseOffset = records.get(0).offset();
                totalSizeEstimate += estimateSizeInBytes(toMagic, baseOffset, batch.compressionType(), records);
                recordBatchAndRecordsList.add(new RecordBatchAndRecords(batch, records, baseOffset));
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSizeEstimate);
        for (RecordBatchAndRecords recordBatchAndRecords : recordBatchAndRecordsList) {
            if (recordBatchAndRecords.batch.magic() <= toMagic)
                recordBatchAndRecords.batch.writeTo(buffer);
            else
                buffer = convertRecordBatch(toMagic, buffer, recordBatchAndRecords);
=======
    @Override
    public Records toMessageFormat(byte toMagic) {
        List<LogEntry> converted = new ArrayList<>();
        for (LogEntry entry : deepEntries(BufferSupplier.NO_CACHING))
            converted.add(LogEntry.create(entry.offset(), entry.record().convert(toMagic)));

        if (converted.isEmpty()) {
            // This indicates that the message is too large, which indicates that the buffer is not large
            // enough to hold a full log entry. We just return all the bytes in the file message set.
            // Even though the message set does not have the right format version, we expect old clients
            // to raise an error to the user after reading the message size and seeing that there
            // are not enough available bytes in the response to read the full message.
            return this;
        } else {
            // We use the first message to determine the compression type for the resulting message set.
            // This could result in message sets which are either larger or smaller than the original size.
            // For example, it could end up larger if most messages were previously compressed, but
            // it just so happens that the first one is not. There is also some risk that this can
            // cause some timestamp information to be lost (e.g. if the timestamp type was changed) since
            // we are essentially merging multiple message sets. However, currently this method is only
            // used for down-conversion, so we've ignored the problem.
            CompressionType compressionType = shallowEntries().iterator().next().record().compressionType();
            return MemoryRecords.withLogEntries(compressionType, converted);
>>>>>>> origin/0.10.2
        }

        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    /**
     * Return a buffer containing the converted record batches. The returned buffer may not be the same as the received
     * one (e.g. it may require expansion).
     */
    private ByteBuffer convertRecordBatch(byte magic, ByteBuffer buffer, RecordBatchAndRecords recordBatchAndRecords) {
        RecordBatch batch = recordBatchAndRecords.batch;
        final TimestampType timestampType = batch.timestampType();
        long logAppendTime = timestampType == TimestampType.LOG_APPEND_TIME ? batch.maxTimestamp() : RecordBatch.NO_TIMESTAMP;

        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, batch.compressionType(),
                timestampType, recordBatchAndRecords.baseOffset, logAppendTime);
        for (Record record : recordBatchAndRecords.records)
            builder.append(record);

        builder.close();
        return builder.buffer();
    }

    /**
     * Get an iterator over the deep records.
     * @return An iterator over the records
     */
    @Override
    public Iterable<Record> records() {
        return records;
    }

    private Iterator<Record> recordsIterator() {
        return new AbstractIterator<Record>() {
            private final Iterator<? extends RecordBatch> batches = batches().iterator();
            private Iterator<Record> records;

            @Override
            protected Record makeNext() {
                if (records != null && records.hasNext())
                    return records.next();

                if (batches.hasNext()) {
                    records = batches.next().iterator();
                    return makeNext();
                }

                return allDone();
            }
        };
    }

    public static int estimateSizeInBytes(byte magic,
                                          long baseOffset,
                                          CompressionType compressionType,
                                          Iterable<Record> records) {
        int size = 0;
        if (magic <= RecordBatch.MAGIC_VALUE_V1) {
            for (Record record : records)
                size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, record.key(), record.value());
        } else {
            size = DefaultRecordBatch.sizeInBytes(baseOffset, records);
        }
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    public static int estimateSizeInBytes(byte magic,
                                          CompressionType compressionType,
                                          Iterable<SimpleRecord> records) {
        int size = 0;
        if (magic <= RecordBatch.MAGIC_VALUE_V1) {
            for (SimpleRecord record : records)
                size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, record.key(), record.value());
        } else {
            size = DefaultRecordBatch.sizeInBytes(records);
        }
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    private static int estimateCompressedSizeInBytes(int size, CompressionType compressionType) {
        return compressionType == CompressionType.NONE ? size : Math.min(Math.max(size / 2, 1024), 1 << 16);
    }

    public static int sizeInBytesUpperBound(byte magic, byte[] key, byte[] value, Header[] headers) {
        return sizeInBytesUpperBound(magic, Utils.wrapNullable(key), Utils.wrapNullable(value), headers);
    }

    public static int sizeInBytesUpperBound(byte magic, ByteBuffer key, ByteBuffer value, Header[] headers) {
        if (magic >= RecordBatch.MAGIC_VALUE_V2)
            return DefaultRecordBatch.batchSizeUpperBound(key, value, headers);
        else
            return Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value);
    }

    private static class RecordBatchAndRecords {
        private final RecordBatch batch;
        private final List<Record> records;
        private final Long baseOffset;

        private RecordBatchAndRecords(RecordBatch batch, List<Record> records, Long baseOffset) {
            this.batch = batch;
            this.records = records;
            this.baseOffset = baseOffset;
        }
    }

}
