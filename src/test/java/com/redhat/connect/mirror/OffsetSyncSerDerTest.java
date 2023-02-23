package com.redhat.connect.mirror;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import com.redhat.connect.mirror.OffsetSyncSerDer.OffsetSync;
import com.redhat.connect.mirror.OffsetSyncSerDer.TopicPartition;

public class OffsetSyncSerDerTest {
    
    @Test
    public void serDer() {
        OffsetSync offsetSync = new OffsetSync(
            new TopicPartition("foo", 1), 
            123, 
            456);

        Pair<ByteBuffer,ByteBuffer> record = OffsetSyncSerDer.serializeRecord(offsetSync);

        assertEquals("foo",OffsetSyncSerDer.deserializeRecord(record).getTopicPartition().getTopic());
        assertEquals(1,OffsetSyncSerDer.deserializeRecord(record).getTopicPartition().getPartition());
        assertEquals(123,OffsetSyncSerDer.deserializeRecord(record).getUpstreamOffset());
        assertEquals(456,OffsetSyncSerDer.deserializeRecord(record).getDownstreamOffset());
    }
}
