package com.github.friendbear.kafka.streams;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;


public class TweetUserCountApp {

    public static Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        // TODO: json Serde
        //final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        //final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        //final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        //KStream<String, String> wordCountInput = builder.stream("word-count-input");
        KStream<String, String> tweetsInput = builder.stream("twitter_tweets");
        //KTable<String, Long> tableOutput = builder.table("twitter_tweets_table");

        var tweetCounts = tweetsInput.selectKey((k, v) -> k)
                .groupByKey()
                .count(Materialized.as("Counts"));

        tweetCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        //tableOutput.toStream().to("twitter_tweets_table");


        return builder.build();

    }
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweet-user-count-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaStreams streams = new KafkaStreams(TweetUserCountApp.createTopology(), properties);
        System.out.println(streams.toString());
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        while(true) {
            streams.metadataForLocalThreads().forEach(data -> {
                System.out.println(data);
            });
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
