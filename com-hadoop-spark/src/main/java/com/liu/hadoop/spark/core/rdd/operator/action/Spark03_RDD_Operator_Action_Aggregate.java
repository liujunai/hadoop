package com.liu.hadoop.spark.core.rdd.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/15 下午6:15
 * @description: RDD 行动算子    aggregate    fold
 * <p>
 * aggregate: 分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
 * fold : 折叠操作，aggregate 的简化版操作
 */
public class Spark03_RDD_Operator_Action_Aggregate {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(5, 6, 1, 2, 3, 4, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list);

		//3.aggregate: 初始值会参与分区内计算,并且和参与分区间计算
		// aggregateByKey : 初始值只会参与分区内计算
		Integer aggregate = rdd.aggregate(0, Integer::sum, Integer::sum);
		System.out.println("aggregate = " + aggregate);

		//4.fold aggregate 的简化版操作
		Integer fold = rdd.fold(0, Integer::sum);
		System.out.println("fold = " + fold);


		jsc.stop();

	}

}
