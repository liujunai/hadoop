package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子   distinct
 * <p>
 * 将数据集中重复的数据去重
 */
public class Spark09_RDD_Operator_Transform_Distinct {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list);

		//3.distinct
		JavaRDD<Integer> rdd1 = rdd.distinct();

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
