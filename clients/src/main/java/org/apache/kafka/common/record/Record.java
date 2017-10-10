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

import java.nio.ByteBuffer;

import org.apache.kafka.common.header.Header;

/**
 * A log record is a tuple consisting of a unique offset in the log, a sequence number assigned by
 * the producer, a timestamp, a key and a value.
 */
public interface Record {

    Header[] EMPTY_HEADERS = new Header[0];

    /**
     * The offset of this record in the log
     * @return the offset
     */
    long offset();

    /**
     * Get the sequence number assigned by the producer.
     * @return the sequence number
     */
    int sequence();

    /**
     * Get the size in bytes of this record.
     * @return the size of the record in bytes
     */
    int sizeInBytes();

    /**
     * Get the record's timestamp.
     * @return the record's timestamp
     */
    long timestamp();

    /**
     * Get a checksum of the record contents.
     * @return A 4-byte unsigned checksum represented as a long or null if the message format does not
     *         include a checksum (i.e. for v2 and above)
     */
    Long checksumOrNull();

    /**
     * Check whether the record has a valid checksum.
     * @return true if the record has a valid checksum, false otherwise
     */
    boolean isValid();

    /**
     * Raise a {@link org.apache.kafka.common.errors.CorruptRecordException} if the record does not have a valid checksum.
     */
    void ensureValid();

    /**
     * Get the size in bytes of the key.
     * @return the size of the key, or -1 if there is no key
     */
    int keySize();

    /**
     * Check whether this record has a key
     * @return true if there is a key, false otherwise
     */
    boolean hasKey();

    /**
     * Get the record's key.
     * @return the key or null if there is none
     */
    ByteBuffer key();

    /**
     * Get the size in bytes of the value.
     * @return the size of the value, or -1 if the value is null
     */
    int valueSize();

    /**
     * Check whether a value is present (i.e. if the value is not null)
     * @return true if so, false otherwise
     */
    boolean hasValue();

    /**
     * Get the record's value
     * @return the (nullable) value
     */
    ByteBuffer value();

    /**
     * Check whether the record has a particular magic. For versions prior to 2, the record contains its own magic,
     * so this function can be used to check whether it matches a particular value. For version 2 and above, this
     * method returns true if the passed magic is greater than or equal to 2.
     *
     * @param magic the magic value to check
     * @return true if the record has a magic field (versions prior to 2) and the value matches
     */
    boolean hasMagic(byte magic);

    /**
     * For versions prior to 2, check whether the record is compressed (and therefore
     * has nested record content). For versions 2 and above, this always returns false.
     * @return true if the magic is lower than 2 and the record is compressed
     */
    boolean isCompressed();

    /**
     * For versions prior to 2, the record contained a timestamp type attribute. This method can be
     * used to check whether the value of that attribute matches a particular timestamp type. For versions
     * 2 and above, this will always be false.
     *
     * @param timestampType the timestamp type to compare
     * @return true if the version is lower than 2 and the timestamp type matches
     */
    boolean hasTimestampType(TimestampType timestampType);

    /**
     * Get the headers. For magic versions 1 and below, this always returns an empty array.
     *
     * @return the array of headers
     */
<<<<<<< HEAD
    Header[] headers();
=======
    public Record convert(byte toMagic) {
        if (toMagic == magic())
            return this;

        ByteBuffer buffer = ByteBuffer.allocate(convertedSize(toMagic));
        TimestampType timestampType = wrapperRecordTimestampType != null ?
                wrapperRecordTimestampType : TimestampType.forAttributes(attributes());
        convertTo(buffer, toMagic, timestamp(), timestampType);
        buffer.rewind();
        return new Record(buffer);
    }

    private void convertTo(ByteBuffer buffer, byte toMagic, long timestamp, TimestampType timestampType) {
        if (compressionType() != CompressionType.NONE)
            throw new IllegalArgumentException("Cannot use convertTo for deep conversion");

        write(buffer, toMagic, timestamp, key(), value(), CompressionType.NONE, timestampType);
    }

    /**
     * Convert this record to another message format and write the converted data to the provided outputs stream.
     *
     * @param out The output stream to write the converted data to
     * @param toMagic The target magic version for conversion
     * @param timestamp The timestamp to use in the converted record (for up-conversion)
     * @param timestampType The timestamp type to use in the converted record (for up-conversion)
     * @throws IOException for any IO errors writing the converted record.
     */
    public void convertTo(DataOutputStream out, byte toMagic, long timestamp, TimestampType timestampType) throws IOException {
        if (compressionType() != CompressionType.NONE)
            throw new IllegalArgumentException("Cannot use convertTo for deep conversion");

        write(out, toMagic, timestamp, key(), value(), CompressionType.NONE, timestampType);
    }

    /**
     * Create a new record instance. If the record's compression type is not none, then
     * its value payload should be already compressed with the specified type; the constructor
     * would always write the value payload as is and will not do the compression itself.
     *
     * @param magic The magic value to use
     * @param timestamp The timestamp of the record
     * @param key The key of the record (null, if none)
     * @param value The record value
     * @param compressionType The compression type used on the contents of the record (if any)
     * @param timestampType The timestamp type to be used for this record
     */
    public static Record create(byte magic,
                                long timestamp,
                                byte[] key,
                                byte[] value,
                                CompressionType compressionType,
                                TimestampType timestampType) {
        int keySize = key == null ? 0 : key.length;
        int valueSize = value == null ? 0 : value.length;
        ByteBuffer buffer = ByteBuffer.allocate(recordSize(magic, keySize, valueSize));
        write(buffer, magic, timestamp, wrapNullable(key), wrapNullable(value), compressionType, timestampType);
        buffer.rewind();
        return new Record(buffer);
    }

    public static Record create(long timestamp, byte[] key, byte[] value) {
        return create(CURRENT_MAGIC_VALUE, timestamp, key, value, CompressionType.NONE, TimestampType.CREATE_TIME);
    }

    public static Record create(byte magic, long timestamp, byte[] key, byte[] value) {
        return create(magic, timestamp, key, value, CompressionType.NONE, TimestampType.CREATE_TIME);
    }

    public static Record create(byte magic, TimestampType timestampType, long timestamp, byte[] key, byte[] value) {
        return create(magic, timestamp, key, value, CompressionType.NONE, timestampType);
    }

    public static Record create(byte magic, long timestamp, byte[] value) {
        return create(magic, timestamp, null, value, CompressionType.NONE, TimestampType.CREATE_TIME);
    }

    public static Record create(byte magic, byte[] key, byte[] value) {
        return create(magic, NO_TIMESTAMP, key, value);
    }

    public static Record create(byte[] key, byte[] value) {
        return create(NO_TIMESTAMP, key, value);
    }

    public static Record create(byte[] value) {
        return create(CURRENT_MAGIC_VALUE, NO_TIMESTAMP, null, value, CompressionType.NONE, TimestampType.CREATE_TIME);
    }

    /**
     * Write the header for a compressed record set in-place (i.e. assuming the compressed record data has already
     * been written at the value offset in a wrapped record). This lets you dynamically create a compressed message
     * set, and then go back later and fill in its size and CRC, which saves the need for copying to another buffer.
     *
     * @param buffer The buffer containing the compressed record data positioned at the first offset of the
     * @param magic The magic value of the record set
     * @param recordSize The size of the record (including record overhead)
     * @param timestamp The timestamp of the wrapper record
     * @param compressionType The compression type used
     * @param timestampType The timestamp type of the wrapper record
     */
    public static void writeCompressedRecordHeader(ByteBuffer buffer,
                                                   byte magic,
                                                   int recordSize,
                                                   long timestamp,
                                                   CompressionType compressionType,
                                                   TimestampType timestampType) {
        int recordPosition = buffer.position();
        int valueSize = recordSize - recordOverhead(magic);

        // write the record header with a null value (the key is always null for the wrapper)
        write(buffer, magic, timestamp, null, null, compressionType, timestampType);

        // now fill in the value size
        buffer.putInt(recordPosition + keyOffset(magic), valueSize);

        // compute and fill the crc from the beginning of the message
        long crc = Utils.computeChecksum(buffer, recordPosition + MAGIC_OFFSET, recordSize - MAGIC_OFFSET);
        Utils.writeUnsignedInt(buffer, recordPosition + CRC_OFFSET, crc);
    }

    private static void write(ByteBuffer buffer,
                              byte magic,
                              long timestamp,
                              ByteBuffer key,
                              ByteBuffer value,
                              CompressionType compressionType,
                              TimestampType timestampType) {
        try {
            DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buffer));
            write(out, magic, timestamp, key, value, compressionType, timestampType);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Write the record data with the given compression type and return the computed crc.
     *
     * @param out The output stream to write to
     * @param magic The magic value to be used
     * @param timestamp The timestamp of the record
     * @param key The record key
     * @param value The record value
     * @param compressionType The compression type
     * @param timestampType The timestamp type
     * @return the computed CRC for this record.
     * @throws IOException for any IO errors writing to the output stream.
     */
    public static long write(DataOutputStream out,
                             byte magic,
                             long timestamp,
                             byte[] key,
                             byte[] value,
                             CompressionType compressionType,
                             TimestampType timestampType) throws IOException {
        return write(out, magic, timestamp, wrapNullable(key), wrapNullable(value), compressionType, timestampType);
    }

    private static long write(DataOutputStream out,
                              byte magic,
                              long timestamp,
                              ByteBuffer key,
                              ByteBuffer value,
                              CompressionType compressionType,
                              TimestampType timestampType) throws IOException {
        byte attributes = computeAttributes(magic, compressionType, timestampType);
        long crc = computeChecksum(magic, attributes, timestamp, key, value);
        write(out, magic, crc, attributes, timestamp, key, value);
        return crc;
    }


    /**
     * Write a record using raw fields (without validation). This should only be used in testing.
     */
    public static void write(DataOutputStream out,
                             byte magic,
                             long crc,
                             byte attributes,
                             long timestamp,
                             byte[] key,
                             byte[] value) throws IOException {
        write(out, magic, crc, attributes, timestamp, wrapNullable(key), wrapNullable(value));
    }

    // Write a record to the buffer, if the record's compression type is none, then
    // its value payload should be already compressed with the specified type
    private static void write(DataOutputStream out,
                              byte magic,
                              long crc,
                              byte attributes,
                              long timestamp,
                              ByteBuffer key,
                              ByteBuffer value) throws IOException {
        if (magic != MAGIC_VALUE_V0 && magic != MAGIC_VALUE_V1)
            throw new IllegalArgumentException("Invalid magic value " + magic);
        if (timestamp < 0 && timestamp != NO_TIMESTAMP)
            throw new IllegalArgumentException("Invalid message timestamp " + timestamp);

        // write crc
        out.writeInt((int) (crc & 0xffffffffL));
        // write magic value
        out.writeByte(magic);
        // write attributes
        out.writeByte(attributes);

        // maybe write timestamp
        if (magic > 0)
            out.writeLong(timestamp);

        // write the key
        if (key == null) {
            out.writeInt(-1);
        } else {
            int size = key.remaining();
            out.writeInt(size);
            out.write(key.array(), key.arrayOffset(), size);
        }
        // write the value
        if (value == null) {
            out.writeInt(-1);
        } else {
            int size = value.remaining();
            out.writeInt(size);
            out.write(value.array(), value.arrayOffset(), size);
        }
    }

    public static int recordSize(byte[] key, byte[] value) {
        return recordSize(CURRENT_MAGIC_VALUE, key, value);
    }

    public static int recordSize(byte magic, byte[] key, byte[] value) {
        return recordSize(magic, key == null ? 0 : key.length, value == null ? 0 : value.length);
    }

    private static int recordSize(byte magic, int keySize, int valueSize) {
        return recordOverhead(magic) + keySize + valueSize;
    }

    // visible only for testing
    public static byte computeAttributes(byte magic, CompressionType type, TimestampType timestampType) {
        byte attributes = 0;
        if (type.id > 0)
            attributes = (byte) (attributes | (COMPRESSION_CODEC_MASK & type.id));
        if (magic > 0)
            return timestampType.updateAttributes(attributes);
        return attributes;
    }

    // visible only for testing
    public static long computeChecksum(byte magic, byte attributes, long timestamp, byte[] key, byte[] value) {
        return computeChecksum(magic, attributes, timestamp, wrapNullable(key), wrapNullable(value));
    }

    /**
     * Compute the checksum of the record from the attributes, key and value payloads
     */
    private static long computeChecksum(byte magic, byte attributes, long timestamp, ByteBuffer key, ByteBuffer value) {
        Crc32 crc = new Crc32();
        crc.update(magic);
        crc.update(attributes);
        if (magic > 0)
            crc.updateLong(timestamp);
        // update for the key
        if (key == null) {
            crc.updateInt(-1);
        } else {
            int size = key.remaining();
            crc.updateInt(size);
            crc.update(key.array(), key.arrayOffset(), size);
        }
        // update for the value
        if (value == null) {
            crc.updateInt(-1);
        } else {
            int size = value.remaining();
            crc.updateInt(size);
            crc.update(value.array(), value.arrayOffset(), size);
        }
        return crc.getValue();
    }

    public static int recordOverhead(byte magic) {
        if (magic == 0)
            return RECORD_OVERHEAD_V0;
        return RECORD_OVERHEAD_V1;
    }

    private static int keyOffset(byte magic) {
        if (magic == 0)
            return KEY_OFFSET_V0;
        return KEY_OFFSET_V1;
    }

>>>>>>> origin/0.10.2
}
