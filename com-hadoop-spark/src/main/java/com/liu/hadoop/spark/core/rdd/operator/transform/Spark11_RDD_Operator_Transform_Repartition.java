package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description:   RDD 转换算子    repartition
 *
 * 该操作内部其实执行的是 coalesce 操作，参数 shuffle 的默认值为 true。无论是将分区数多的
 * RDD 转换为分区数少的 RDD，还是将分区数少的 RDD 转换为分区数多的 RDD，repartition
 * 操作都可以完成，因为无论如何都会经 shuffle 过程。
 */
public class Spark11_RDD_Operator_Transform_Repartition {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list,4);

		//3.repartition
		// coalesce算子可以扩大分区的，但是如果不进行shuffle操作，是没有意义，不起作用。
		// 所以如果想要实现扩大分区的效果，需要使用shuffle操作
		// spark提供了一个简化的操作
		// 缩减分区：coalesce，如果想要数据均衡，可以采用shuffle
		// 扩大分区：repartition, 底层代码调用的就是coalesce，而且肯定采用shuffle
		JavaRDD<Integer> rdd1 = rdd.repartition(2);

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
