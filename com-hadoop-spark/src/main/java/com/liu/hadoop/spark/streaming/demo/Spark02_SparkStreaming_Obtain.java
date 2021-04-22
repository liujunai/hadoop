package com.liu.hadoop.spark.streaming.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

/**
 * @author LiuJunFeng
 * @date 2021/4/22 下午12:48
 * @description: 获取模拟数据
 */
public class Spark02_SparkStreaming_Obtain {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("demo");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "liu1:9092,liu2:9092,liu3:9092");
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "spark");
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

		JavaInputDStream<ConsumerRecord<String, String>> kafkaRdd = KafkaUtils.createDirectStream(
				jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(Collections.singletonList("liu"), kafkaParams));

		kafkaRdd.map(new Function<ConsumerRecord<String, String>, String>() {
			@Override
			public String call(ConsumerRecord<String, String> v1) throws Exception {
				return v1.value();
			}
		}).print();


		jssc.start();

		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}


}
