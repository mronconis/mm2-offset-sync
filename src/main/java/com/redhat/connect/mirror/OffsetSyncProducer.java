package com.redhat.connect.mirror;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.redhat.connect.mirror.OffsetSyncSerDer.OffsetSync;

public class OffsetSyncProducer {

    private Properties props;
    private KafkaProducer<byte[], byte[]> producer;

    public OffsetSyncProducer(String...args) throws IOException{
        try (InputStream is = OffsetSyncProducer.class.getResourceAsStream("/producer.properties" )) {
            Properties props = initProps(is, args);
            producer = new KafkaProducer<byte[], byte[]>(props);
        }
    }

    private Properties initProps(InputStream is, String...args) throws IOException {
        props = new Properties();
        props.load(is);
        if (args != null) {
            if(args[0] != null) 
                props.setProperty("bootstrap.servers", args[0]);
            if (args[1] != null) 
                props.setProperty("topic", args[1]);
        }
        return props;
    }
    
    public Future<RecordMetadata> send(OffsetSync record) throws IOException{
        Pair<ByteBuffer, ByteBuffer> keyValue = OffsetSyncSerDer.serializeRecord(record);
        ProducerRecord<byte[], byte[]> data = new ProducerRecord<byte[], byte[]>(props.getProperty("topic"), keyValue.getKey().array(), keyValue.getValue().array());
        return producer.send(data);
    }

    public void close() {
        producer.close();
    }
}
