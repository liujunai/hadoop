package com.liu.hadoop.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * @author LiuJunFeng
 * @date 2021/4/20 下午4:54
 * @description: SparkStreaming  stateless / stateful
 *
 * 1  stateless: 无状态转化操作
 * 无状态转化操作就是把简单的 RDD 转化操作应用到每个批次上，也就是转化 DStream 中的每一个 RDD。部分无状态转化操作列在了下表中
 *
 * 2 stateful: 有状态转化操作
 * UpdateStateByKey 原语用于记录历史记录，有时，我们需要在 DStream 中跨批次维护状态(例如流计算中累加 wordcount)。针对这种情况，
 * updateStateByKey()为我们提供了对一个状态变量的访问，用于键值对形式的 DStream。给定一个由(键，事件)对构成的 DStream，
 * 并传递一个指定如何根据新的事件更新每个键对应状态的函数，它可以构建出一个新的 DStream，其内部数据为(键，状态) 对。
 * updateStateByKey() 的结果会是一个新的 DStream，其内部的 RDD 序列是由每个时间区间对应的(键，状态)对组成的。
 *
 */
public class Spark04_SparkStreaming_State {

	public static void main(String[] args) {

		// 创建spark 的运行环境 SparkStreaming
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming");
		// 第一个参数表示环境配置
		// 第二个参数表示批量处理的周期（采集周期） minutes 分钟  seconds 秒  milliseconds 毫秒
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

		// 无状态数据操作，只对当前的采集周期内的数据进行处理
		// 在某些场合下，需要保留数据统计结果（状态），实现数据的汇总
		// 使用有状态操作时，需要设定检查点路径
		jssc.checkpoint("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/stateful");

		JavaReceiverInputDStream<String> localhost = jssc.socketTextStream("localhost", 8888);

		JavaDStream<String> words = localhost.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

		JavaPairDStream<String, Integer> wordToOne = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<>(s, 1);
			}
		});

		// updateStateByKey：根据key对数据的状态进行更新
		// 传递的参数中含有两个值
		// 第一个值表示相同的key的value数据
		// 第二个值表示缓存区相同key的value数据
		JavaPairDStream<String, Integer> javaPairDStream = wordToOne.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
			@Override
			public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
				//第一个参数就是key传进来的数据，第二个参数是曾经已有的数据
				//如果第一次，state没有，updatedValue为0，如果有，就获取
				Integer updatedValue = 0;
				if (state.isPresent()) {
					updatedValue = state.get();
				}

				//遍历batch传进来的数据可以一直加，随着时间的流式会不断去累加相同key的value的结果。
				for (Integer value : values) {
					updatedValue += value;
				}

				//返回更新的值
				return Optional.of(updatedValue);
			}
		});

		javaPairDStream.print();

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
