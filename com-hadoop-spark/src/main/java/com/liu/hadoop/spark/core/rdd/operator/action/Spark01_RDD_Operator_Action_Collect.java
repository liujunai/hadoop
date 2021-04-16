package com.liu.hadoop.spark.core.rdd.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/15 下午6:15
 * @description:   RDD 行动算子    collect
 *
 * 在驱动程序中，以数组 Array 的形式返回数据集的所有元素
 */
public class Spark01_RDD_Operator_Action_Collect {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list);

		// collect  行动算子
		// 所谓的行动算子，其实就是触发作业(Job)执行的方法
		// 底层代码调用的是环境对象的runJob方法
		// 底层代码中会创建ActiveJob，并提交执行。
		// 方法会将不同分区的数据按照分区顺序采集到Driver端内存中，形成数组
		rdd.collect();

		jsc.stop();

	}

}
