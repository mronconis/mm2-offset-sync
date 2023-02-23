# Mirror maker 2 offset sync
The project is useful to manual sync the data of the Mirror Maker 2 topic "mm2-offset-sync" from one kafka cluster to another.


## Export offset-sync data
```
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server <bootstrap-servers_source> \
    --topic <topic> \
    --formatter org.apache.kafka.connect.mirror.formatters.OffsetSyncFormatter \
    --from-beginning > offset-sync.data
```

## Import offset-sync data
```
cat offset-sync.data | java -jar target/offset-sync.jar <bootstrap-servers_target> <topic>
```