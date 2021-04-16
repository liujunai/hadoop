package com.liu.hadoop.spark.core.accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/13 下午7:45
 * @description: RDD  累加器   分布式共享只写变量
 * <p>
 * 累加器用来把 Executor 端变量信息聚合到 Driver 端。在 Driver 程序中定义的变量，在
 * Executor 端的每个 Task 都会得到这个变量的一份新的副本，每个 task 更新这些副本的值后，
 * 传回 Driver 端进行 merge。
 */
public class Spark02_RDD_Accumulator {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list);

		// LongAccumulator: 数值型累加
		LongAccumulator acc = jsc.sc().longAccumulator("long-account");

		// 使用累加器
		JavaRDD<Integer> map = rdd.map(x -> {
			acc.add(x);
			return x;
		});

		// 获取累加器的值
		// 少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行

		// 多加：行动算子中调用累加器，如果执行多次行动算子，会累加多次
		map.collect();
		map.collect();
		// 一般情况下，累加器会放置在行动算子进行操作


		//4.获取累加器的值输出结果
		System.out.println("acc = " + acc.value());


		//5.关闭资源
		jsc.close();

	}


}
