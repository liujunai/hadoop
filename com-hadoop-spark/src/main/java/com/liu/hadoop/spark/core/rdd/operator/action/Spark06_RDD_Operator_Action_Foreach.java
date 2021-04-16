package com.liu.hadoop.spark.core.rdd.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/15 下午6:15
 * @description: RDD 行动算子    foreach
 * <p>
 * 分布式遍历 RDD 中的每一个元素，调用指定函数
 */
public class Spark06_RDD_Operator_Action_Foreach {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建  PairRDD
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<>("class1", 90),
				new Tuple2<>("class2", 60),
				new Tuple2<>("class2", 60),
				new Tuple2<>("class1", 60),
				new Tuple2<>("class1", 60),
				new Tuple2<>("class2", 50)
		);
		JavaPairRDD<String, Integer> rdd = jsc.parallelizePairs(scoreList);

		//3.foreach
		// 在Executor端执行  foreach 其实是Executor端内存数据打印
		rdd.foreach(x -> System.out.println("x = " + x));

		System.out.println("--------------------------------------------------------");

		// 在Driver端执行  foreach 其实是Driver端内存集合的循环遍历方法
		rdd.collect().forEach(x -> System.out.println("x = " + x));


		jsc.stop();

	}

}
