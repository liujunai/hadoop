package com.liu.hadoop.spark.core.rdd.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/15 下午6:15
 * @description: RDD 行动算子    reduce   count  first   takeOrdered
 * <p>
 * reduce: 聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据
 * count: 返回 RDD 中元素的个数
 * first: 返回 RDD 中的第一个元素
 * take: 返回一个由 RDD 的前 n 个元素组成的数组
 * takeOrdered: 返回该 RDD 排序后的前 n 个元素组成的数组
 */
public class Spark02_RDD_Operator_Action_Reduce {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(5, 6, 1, 2, 3, 4, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list);

		//reduce: 聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据
		Integer reduce = rdd.reduce(Integer::sum);
		System.out.println("reduce = " + reduce);

		//count : 数据源中数据的个数
		long count = rdd.count();
		System.out.println("count = " + count);

		//first : 获取数据源中数据的第一个
		Integer first = rdd.first();
		System.out.println("first = " + first);

		//take : 获取前N个数据
		List<Integer> take = rdd.take(3);
		System.out.println("take = " + take);

		// takeOrdered : 数据排序后，取N个数据
		List<Integer> takeOrdered = rdd.takeOrdered(3);
		System.out.println("takeOrdered = " + takeOrdered);

		jsc.stop();

	}

}
