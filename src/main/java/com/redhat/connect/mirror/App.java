package com.redhat.connect.mirror;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.redhat.connect.mirror.OffsetSyncSerDer.OffsetSync;

public class App {
    public static void main( String[] args ) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            OffsetSyncProducer producer = new OffsetSyncProducer(args);
            String line = null;
            while (true) {
                if ((line = reader.readLine()) != null) {
                    System.out.println("send>> " + line);
                    OffsetSync record = OffsetSyncParser.parse(line);
                    if (record != null)
                        producer.send(record);
                } else {
                    producer.close();
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
