package com.redhat.connect.mirror;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.tuple.Pair;

public class OffsetSyncSerDer {

    @Data
    @AllArgsConstructor
    static class TopicPartition {
        private String topic;
        private int partition;
    }

    @Data
    @AllArgsConstructor
    static class OffsetSync {
        private TopicPartition topicPartition;
        private long upstreamOffset;
        private long downstreamOffset;
    }
    
    public static final String TOPIC_KEY = "topic";
    public static final String PARTITION_KEY = "partition";
    public static final String UPSTREAM_OFFSET_KEY = "upstreamOffset";
    public static final String DOWNSTREAM_OFFSET_KEY = "offset";

    public static final Schema VALUE_SCHEMA = new Schema(
        new Field(UPSTREAM_OFFSET_KEY, Type.INT64),
        new Field(DOWNSTREAM_OFFSET_KEY, Type.INT64));

    public static final Schema KEY_SCHEMA = new Schema(
        new Field(TOPIC_KEY, Type.STRING),
        new Field(PARTITION_KEY, Type.INT32));

    public static OffsetSync deserializeRecord(Pair<ByteBuffer,ByteBuffer> record) {
        Struct keyStruct = KEY_SCHEMA.read(ByteBuffer.wrap(record.getKey().array()));
        String topic = keyStruct.getString(TOPIC_KEY);
        int partition = keyStruct.getInt(PARTITION_KEY);

        Struct valueStruct = VALUE_SCHEMA.read(ByteBuffer.wrap(record.getValue().array()));
        long upstreamOffset = valueStruct.getLong(UPSTREAM_OFFSET_KEY);
        long downstreamOffset = valueStruct.getLong(DOWNSTREAM_OFFSET_KEY);

        return new OffsetSync(new TopicPartition(topic, partition), upstreamOffset, downstreamOffset);
    }

    public static Pair<ByteBuffer,ByteBuffer> serializeRecord(OffsetSync line) {
        Struct key = new Struct(KEY_SCHEMA);
        key.set(TOPIC_KEY, line.getTopicPartition().getTopic());
        key.set(PARTITION_KEY, line.getTopicPartition().getPartition());

        Struct value = new Struct(VALUE_SCHEMA);
        value.set(UPSTREAM_OFFSET_KEY, line.getUpstreamOffset());
        value.set(DOWNSTREAM_OFFSET_KEY, line.getDownstreamOffset());
        
        ByteBuffer keyBuffer = serializeKey(key);
        ByteBuffer valueBuffer = serializeValue(value);
        
        return Pair.of(keyBuffer, valueBuffer);
    }

    private static ByteBuffer serializeValue(Struct value) {
        ByteBuffer buffer = ByteBuffer.allocate(VALUE_SCHEMA.sizeOf(value));
        VALUE_SCHEMA.write(buffer, value);
        buffer.flip();
        return buffer;
    }

    private static ByteBuffer serializeKey(Struct key) {
        ByteBuffer buffer = ByteBuffer.allocate(KEY_SCHEMA.sizeOf(key));
        KEY_SCHEMA.write(buffer, key);
        buffer.flip();
        return buffer;
    }
}
