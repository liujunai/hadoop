package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子    groupByKey
 * <p>
 * 将数据源的数据根据 key 对 value 进行分组
 */
public class Spark16_RDD_Operator_Transform_GroupByKey {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建  PairRDD
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 60),
				new Tuple2<String, Integer>("class2", 60),
				new Tuple2<String, Integer>("class1", 60),
				new Tuple2<String, Integer>("class1", 60),
				new Tuple2<String, Integer>("class2", 50)
		);
		//平行化集合 生成JavaPairRDD  此处使用的是parallelizePairs
		JavaPairRDD<String, Integer> rdd = jsc.parallelizePairs(scoreList);

		//3.groupByKey : 将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
		//     			 元组中的第一个元素就是key，
		//     			 元组中的第二个元素就是相同key的value的集合
		JavaPairRDD<String, Iterable<Integer>> rdd1 = rdd.groupByKey();

		//4.执行任务
		List<Tuple2<String, Iterable<Integer>>> collect = rdd1.collect();

		//5.输出结果
		collect.forEach(x -> System.out.println("key = " + x._1 + "  value = " + x._2));

		//6.关闭资源
		jsc.close();
	}

}
