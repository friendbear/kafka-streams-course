## create topics

```shell

kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic favourite-colour-input

kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic user-keys-and-colours --config cleanup.policy=compact

kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic favourite-colour-output
```

### producer

```shell
kafka-console-producer.sh --broker-list localhost:9092 --topic favourite-colour-input
>name,green
>name,red
>name,red
>jon,green
>kumagai,red
>kuma,red
```

###  consumer

```shell
kafka-console-consumer.sh --bootstrap-server localhost:9092     --topic favourite-colour-output     --from-beginning     --formatter kafka.tools.DefaultMessageFormatter     --property print.key=true     --property print.value=true     --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer     --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
green	1
green	0
red	1
red	0
red	1
green	1
red	2
red	3
```