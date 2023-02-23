package com.redhat.connect.mirror;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.redhat.connect.mirror.OffsetSyncSerDer.OffsetSync;
import com.redhat.connect.mirror.OffsetSyncSerDer.TopicPartition;

@Testcontainers
public class OffsetSyncProducerTest {
    @Container
    public KafkaContainer container = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

    @Test
    void send() throws IOException, InterruptedException, ExecutionException {
        String[] args = {container.getBootstrapServers(),"foo"};
        OffsetSyncProducer producer = new OffsetSyncProducer(args);

        OffsetSync offsetSync = new OffsetSync(
            new TopicPartition("foo", 1), 
            123, 
            456);

        Future<RecordMetadata> future = producer.send(offsetSync);

        assertEquals(9, future.get().serializedKeySize());
        assertEquals(16, future.get().serializedValueSize());
    }
}
