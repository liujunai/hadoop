package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description:   RDD 转换算子   map
 *
 * 将处理的数据逐条进行映射转换，这里的转换可以是类型的转换，也可以是值的转换。
 */
public class Spark01_RDD_Operator_Transform_Map {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list);

		//3.map
		JavaRDD<Integer> rdd1 = rdd.map(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer integer) throws Exception {
				return integer * 2;
			}
		});

		// 1. rdd的计算一个分区内的数据是一个一个执行逻辑
		//    只有前面一个数据全部的逻辑执行完毕后，才会执行下一个数据。
		//    分区内数据的执行是有序的。
		// 2. 不同分区数据计算是无序的。


		//4.执行任务
		List<Integer> collect = rdd1.collect();

		//5.输出结果
		for (Integer integer : collect) {
			System.out.println("integer = " + integer);
		}

		//6.关闭资源
		jsc.close();
	}

}
