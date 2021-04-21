package com.liu.hadoop.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * @author LiuJunFeng
 * @date 2021/4/20 下午4:54
 * @description: SparkStreaming  stateless无状态   join
 *
 * 1  stateless: 无状态转化操作
 * 无状态转化操作就是把简单的 RDD 转化操作应用到每个批次上，也就是转化 DStream 中的每一个 RDD。部分无状态转化操作列在了下表中
 *
 * join: 两个流之间的 join 需要两个流的批次大小一致，这样才能做到同时触发计算。计算过程就是
 * 对当前批次的两个流中各自的 RDD 进行 join，与两个 RDD 的 join 效果相同。
 *
 */
public class Spark06_SparkStreaming_Stateless_Join {

	public static void main(String[] args) {

		// 创建spark 的运行环境 SparkStreaming
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming");
		// 第一个参数表示环境配置
		// 第二个参数表示批量处理的周期（采集周期） minutes 分钟  seconds 秒  milliseconds 毫秒
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(8));

		JavaReceiverInputDStream<String> rdd1 = jssc.socketTextStream("localhost", 8888);
		JavaReceiverInputDStream<String> rdd2 = jssc.socketTextStream("localhost", 9999);

		JavaPairDStream<String, Integer> mapRdd1 = rdd1.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s,8));

		JavaPairDStream<String, Integer> mapRdd2 = rdd2.mapToPair(x -> new Tuple2<>(x, 9));

		mapRdd1.print();
		mapRdd2.print();

		// 所谓的DStream的Join操作，其实就是两个RDD的join 将相同key 的value聚合在一起
		JavaPairDStream<String, Tuple2<Integer, Integer>> joinRdd = mapRdd1.join(mapRdd2);

		joinRdd.print();


		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
