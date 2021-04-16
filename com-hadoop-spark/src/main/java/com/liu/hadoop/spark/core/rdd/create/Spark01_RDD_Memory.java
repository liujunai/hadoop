package com.liu.hadoop.spark.core.rdd.create;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/13 下午7:45
 * @description:  从集合（内存）中创建 RDD
 */
public class Spark01_RDD_Memory {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD，将内存中集合的数据作为处理的数据源 并行
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list);

		//3.执行任务
		List<Integer> collect = rdd.collect();

		//4.输出结果
		for (Integer integer : collect) {
			System.out.println("integer = " + integer);
		}

		//5.关闭资源
		jsc.close();

	}


}
