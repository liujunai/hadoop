package com.liu.hadoop.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * @author LiuJunFeng
 * @date 2021/4/20 下午4:54
 * @description: SparkStreaming   输出 Output
 * <p>
 * 输出操作指定了对流数据经转化操作得到的数据所要执行的操作(例如把结果推入外部数据库
 * 或输出到屏幕上)。与 RDD 中的惰性求值类似，如果一个 DStream 及其派生出的 DStream 都没
 * 有被执行输出操作，那么这些 DStream 就都不会被求值。如果 StreamingContext 中没有设定输出
 * 操作，整个 context 就都不会启动。
 */
public class Spark09_SparkStreaming_Output {

	public static void main(String[] args) {

		// 创建spark 的运行环境 SparkStreaming
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming");
		// 第一个参数表示环境配置
		// 第二个参数表示批量处理的周期（采集周期） minutes 分钟  seconds 秒  milliseconds 毫秒
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

		JavaReceiverInputDStream<String> localhost = jssc.socketTextStream("localhost", 8888);

		// SparkStreaming如何没有输出操作，那么会提示错误
//		localhost.print();

		// foreachRDD不会出现时间戳
		localhost.foreachRDD((VoidFunction<JavaRDD<String>>) stringJavaRDD -> System.out.println("stringJavaRDD = " + stringJavaRDD));

		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
