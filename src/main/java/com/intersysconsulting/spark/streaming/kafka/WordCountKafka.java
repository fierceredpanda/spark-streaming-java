package com.intersysconsulting.spark.streaming.kafka;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Sample word count that pulls data from Kafka.
 *
 * @author Victor M. Miller
 */
public class WordCountKafka {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        String master = "local[*]";
        String topic = "awesome";
        String kafkaLocation = "localhost:2181";

        if (args.length == 3) {
            master = args[2];
        }

        if (args.length >= 2) {
            kafkaLocation = args[1];
        }

        if (args.length >= 1) {
            topic = args[0];
        }

        SparkConf conf = new SparkConf().setAppName("WordCountKafka").setMaster(master);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

        Map<String, Integer> topicsMap = new HashMap<>();
        topicsMap.put(topic, 1);
        JavaPairReceiverInputDStream<String, String> inputStream = KafkaUtils.createStream(ssc, kafkaLocation, "WordCountKafka", topicsMap);

        JavaDStream<String> messages = inputStream.map(Tuple2::_2);
        JavaDStream<String> words = messages.flatMap(message -> Arrays.asList(SPACE.split(message)))
                .map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase());
        JavaPairDStream<String, Integer> countedWords = words.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
        countedWords.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
