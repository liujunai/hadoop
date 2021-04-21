package com.liu.hadoop.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author LiuJunFeng
 * @date 2021/4/20 下午4:54
 * @description: SparkStreaming  的 Receiver
 */
public class Spark02_SparkStreaming_Receiver {

	public static void main(String[] args) {

		// 创建spark 的运行环境 SparkStreaming
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming");
		// 第一个参数表示环境配置
		// 第二个参数表示批量处理的周期（采集周期） minutes 分钟  seconds 秒  milliseconds 毫秒
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));


		// 使用自定义数据采集器  传递参数 存储级别
		JavaReceiverInputDStream<String> receiverRdd = jssc.receiverStream(
				new MyReceiver(StorageLevel.MEMORY_ONLY(),"localhost",8888));

		JavaDStream<String> words = receiverRdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

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
