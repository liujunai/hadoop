package com.liu.hadoop.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.*;

/**
 * @author LiuJunFeng
 * @date 2021/4/20 下午4:54
 * @description: SparkStreaming  获取 Kafka 数据源
 *
 * ReceiverAPI：需要一个专门的 Executor 去接收数据，然后发送给其他的 Executor 做计算。存在
 * 的问题，接收数据的 Executor 和计算的 Executor 速度会有所不同，特别在接收数据的 Executor
 * 速度大于计算的 Executor 速度，会导致计算数据的节点内存溢出。早期版本中提供此方式，当前版本不适用
 *
 * DirectAPI：是由计算的 Executor 来主动消费 Kafka 的数据，速度由自身控制。
 *
 */
public class Spark03_SparkStreaming_Kafka {

	public static void main(String[] args) {

		// 创建spark 的运行环境 SparkStreaming
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming");
		// 第一个参数表示环境配置
		// 第二个参数表示批量处理的周期（采集周期） minutes 分钟  seconds 秒  milliseconds 毫秒
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

		// kafka 配置
		// 指定topic
		String topics = "liu";
		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		// kafka 连接参数
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "liu1:9092,liu2:9092,liu3:9092");
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "spark");
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);


		// 使用 kafka 数据采集器  SparkStreaming环境
		JavaInputDStream<ConsumerRecord<String, String>> kafkaRdd = KafkaUtils.createDirectStream(
				jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(topicsSet, kafkaParams));


		JavaDStream<String> words = kafkaRdd.flatMap(s -> Arrays.asList(s.value().split(" ")).iterator());

		JavaPairDStream<String, Integer> wordToOne = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<>(s, 1);
			}
		});

		JavaPairDStream<String, Integer> wordToCount = wordToOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer integer, Integer integer2) throws Exception {
				return integer + integer2;
			}
		});

		wordToCount.print();


		// 由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
		// 如果main方法执行完毕，应用程序也会自动结束。所以不能让main执行完毕
		//jssc.stop();
		// 1. 启动采集器
		jssc.start();
		try {
			// 2. 等待采集器的关闭
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
