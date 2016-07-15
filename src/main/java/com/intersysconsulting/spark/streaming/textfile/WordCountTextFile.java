package com.intersysconsulting.spark.streaming.textfile;

import com.google.common.collect.Queues;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Queue;

/**
 * Word count based off a text file that is read in and fed to the spark streaming application.
 *
 * @author Victor M. Miller
 */
public class WordCountTextFile {

    public static void main(String[] args) {

        String master = "local[*]";
        String path = "";

        if (args.length == 2) {
            master = args[1];
        }

        if (args.length >= 1) {
            path = args[0];
        }

        Logger.getRootLogger().setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("WordCountExtFile").setMaster(master);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

        JavaRDD<String> sampleRDD = ssc.sparkContext().textFile(path);
        Queue<JavaRDD<String>> queue = Queues.newLinkedBlockingQueue();
        queue.add(sampleRDD);
        JavaDStream<String> sampleDStream = ssc.queueStream(queue);

        JavaDStream<String> wordDStream = sampleDStream.flatMap(s -> Arrays.asList(s.split(" ")))
                .map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase());
        JavaPairDStream<String, Integer> initialWordCount = wordDStream.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> countedWords = initialWordCount.reduceByKey((i1, i2) -> i1 + i2);
        countedWords.foreachRDD((JavaPairRDD<String, Integer> rdd) -> rdd.foreach(tuple -> System.out.println(tuple._1 + " : " + tuple._2)));

        ssc.start();
        ssc.awaitTermination();
    }
}
