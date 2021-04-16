package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子     intersection   union   subtract   zip
 * <p>
 * intersection: 对源 RDD 和参数 RDD 求交集后返回一个新的 RDD
 * union: 对源 RDD 和参数 RDD 求并集后返回一个新的 RDD
 * subtract: 以一个 RDD 元素为主，去除两个 RDD 中重复元素，将其他元素保留下来。求差集
 * zip: 将两个 RDD 中的元素，以键值对的形式进行合并。其中，键值对中的 Key 为第 1 个 RDD 中的元素，Value 为第 2 个 RDD 中的相同位置的元素。
 */
public class Spark13_RDD_Operator_Transform_Intersection {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		List<Integer> list2 = Arrays.asList(3, 4, 5, 6, 7, 8, 9);
		JavaRDD<Integer> rdd1 = jsc.parallelize(list1);
		JavaRDD<Integer> rdd2 = jsc.parallelize(list2);

		//3. 交集、并集、差集: 要求两个数据源数据类型保持一致
		//   拉链操作: 两个数据源的类型可以不一致

		// intersection  交集    去重
		JavaRDD<Integer> intersection = rdd1.intersection(rdd2);
		intersection.collect().forEach(x -> System.out.println("intersection = " + x ));

		// union  并集			不去重
		JavaRDD<Integer> union = rdd1.union(rdd2);
		union.collect().forEach(x -> System.out.println("union = " + x ));

		// subtract  差集   		不去重
		JavaRDD<Integer> subtract = rdd1.subtract(rdd2);
		subtract.collect().forEach(x -> System.out.println("subtract = " + x ));

		// zip  差集				两个数据源要求分区数量、分区中数据数量保持一致
		JavaPairRDD<Integer, Integer> zip = rdd1.zip(rdd2);
		zip.collect().forEach(x -> System.out.println("zip = " + x ));

		//6.关闭资源
		jsc.close();
	}

}
