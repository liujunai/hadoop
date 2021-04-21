package com.liu.hadoop.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
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
 * @description: SparkStreaming  stateful 有状态  WindowOperations
 * <p>
 * 2 stateful: 有状态转化操作
 * Window Operations 可以设置窗口的大小和滑动窗口的间隔来动态的获取当前 Steaming 的允许状态。所有基于窗口的操作都需要两个参数，分别为窗口时长以及滑动步长。
 * 窗口时长：计算内容的时间范围
 * 滑动步长：隔多久触发一次计算
 * 注意：这两者都必须为采集周期大小的整数倍
 */
public class Spark08_SparkStreaming_Stateful_WindowOperations {

	public static void main(String[] args) {

		// 创建spark 的运行环境 SparkStreaming
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming");
		// 第一个参数表示环境配置
		// 第二个参数表示批量处理的周期（采集周期） minutes 分钟  seconds 秒  milliseconds 毫秒
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

		jssc.checkpoint("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/stateful");

		JavaReceiverInputDStream<String> localhost = jssc.socketTextStream("localhost", 8888);

		// 窗口的范围应该是采集周期的整数倍
		// 窗口可以滑动的，但是默认情况下，一个采集周期进行滑动
		// 这样的话，可能会出现重复数据的计算，为了避免这种情况，可以改变滑动的滑动（步长）
		JavaDStream<String> window = localhost.window(Durations.seconds(6), Durations.seconds(6));

/**
 * （1）window(windowLength, slideInterval): 基于对源 DStream 窗化的批次进行计算返回一个新的 Dstream；
 *
 * （2）countByWindow(windowLength, slideInterval): 返回一个滑动窗口计数流中的元素个数；
 *
 * （3）reduceByWindow(func, windowLength, slideInterval): 通过使用自定义函数整合滑动区间流元素来创建一个新的单元素流；
 *
 * （4）reduceByKeyAndWindow(func, windowLength, slideInterval, [numTasks]): 当在一个(K,V)对的 DStream 上调用此函数，
 *   会返回一个新(K,V)对的 DStream，此处通过对滑动窗口中批次数据使用 reduce 函数来整合每个 key 的 value 值。
 *
 * （5）reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks]): 这个函数是上述函数的变化版本，
 *   每个窗口的 reduce 值都是通过用前一个窗的 reduce 值来递增计算。通过 reduce 进入到滑动窗口数据并”反向 reduce”离开窗口的旧数据来实现这个操作。
 *   一个例子是随着窗口滑动对 keys 的“加”“减”计数。通过前边介绍可以想到，这个函数只适用于”
 *
 */
		JavaPairDStream<String, Integer> mapToPair = localhost.mapToPair(x -> new Tuple2<>(x, 1));
		// reduceByKeyAndWindow : 当窗口范围比较大，但是滑动幅度比较小，那么可以采用增加数据和删除数据的方式
		// 无需重复计算，提升性能。
		mapToPair.reduceByKeyAndWindow(
				// 滑动进来的新值 操作
				new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				},
				// 滑动出去的值 操作
				new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 - v2;
					}
				},
				// 滑动 窗口时长
				Durations.seconds(9),
				// 滑动 步长
				Durations.seconds(3)
		);

		window.print();


		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
