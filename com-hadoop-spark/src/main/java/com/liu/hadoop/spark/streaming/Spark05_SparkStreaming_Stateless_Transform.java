package com.liu.hadoop.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/20 下午4:54
 * @description: SparkStreaming  stateless无状态  Transform
 *
 * 1  stateless: 无状态转化操作
 * 无状态转化操作就是把简单的 RDD 转化操作应用到每个批次上，也就是转化 DStream 中的每一个 RDD。部分无状态转化操作列在了下表中
 *
 * Transform: 允许 DStream 上执行任意的 RDD-to-RDD 函数。即使这些函数并没有在 DStream的 API 中暴露出来，
 * 通过该函数可以方便的扩展 Spark API。该函数每一批次调度一次。其实也就是对 DStream 中的 RDD 应用转换。
 *
 */
public class Spark05_SparkStreaming_Stateless_Transform {

	public static void main(String[] args) {

		// 创建spark 的运行环境 SparkStreaming
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming");
		// 第一个参数表示环境配置
		// 第二个参数表示批量处理的周期（采集周期） minutes 分钟  seconds 秒  milliseconds 毫秒
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

		JavaReceiverInputDStream<String> rdd = jssc.socketTextStream("localhost", 8888);

		// transform方法可以将底层RDD获取到后进行操作
		// 使用场景
		// 1. DStream功能不完善
		// 2. 需要代码周期性的执行

		// Code : Driver端
		JavaDStream<String> map1 = rdd.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
			@Override
			public JavaRDD<String> call(JavaRDD<String> stringJavaRDD) throws Exception {
				// Code : Driver端，（周期性执行）
				return stringJavaRDD;
			}
		}).map(new Function<String, String>() {
			@Override
			public String call(String s) throws Exception {
				// Code : Executor端
				return s;
			}
		});

		// Code : Driver端
		JavaDStream<String> map = rdd.map(new Function<String, String>() {
			@Override
			public String call(String s) throws Exception {
				// Code : Executor端
				return s;
			}
		});


		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
