package com.liu.hadoop.spark.core.rdd.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author LiuJunFeng
 * @date 2021/4/15 下午6:15
 * @description: RDD 行动算子    countByKey   countByValue
 * <p>
 * countByKey: 统计每种 key 的个数
 * countByValue: 统计每种 value 的个数
 */
public class Spark04_RDD_Operator_Action_CountByKey {

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

		//3.countByKey 统计每种 key 的个数
		Map<String, Long> stringLongMap = rdd.countByKey();
		System.out.println("stringLongMap = " + stringLongMap);

		jsc.stop();

	}

}
