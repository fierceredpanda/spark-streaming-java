package com.intersysconsulting.spark.streaming.textfile;

import com.google.common.collect.Queues;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Queue;

/**
 * Sample project that streams data in from a text file and does a basic word count on it.
 *
 * @author Victor M. Miller
 */
public class WordCountTextFileByRDD {

    public static void main(String[] args) {

        String master = "local[*]";
        String path = "";

        if (args.length == 2) {
            master = args[1];
        }

        if (args.length >= 1) {
            path = args[0];
        }

        SparkConf conf = new SparkConf().setAppName("WordCountExtFile").setMaster(master);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

        JavaRDD<String> sampleRDD = ssc.sparkContext().textFile(path);
        Queue<JavaRDD<String>> queue = Queues.newLinkedBlockingQueue();
        queue.add(sampleRDD);
        JavaDStream<String> sampleDStream = ssc.queueStream(queue);

        //Translate the sample data into individual words
        sampleDStream.foreachRDD(rdd -> {
            JavaRDD<String> wordDStream = rdd.flatMap(s -> Arrays.asList(s.split(" ")))
                    .map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase());;
            JavaPairRDD<String, Integer> initialWordCount = wordDStream.mapToPair(word -> new Tuple2<>(word, 1));
            JavaPairRDD<String, Integer> countedWords = initialWordCount.reduceByKey((i, i1) -> i + i1);

            countedWords.collect().forEach(tuple -> System.out.println(tuple._1 + " -> " + tuple._2));
        });

        ssc.start();
        ssc.awaitTermination(1500);
    }
}
