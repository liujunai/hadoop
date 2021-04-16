package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子    foldByKey
 * <p>
 * 当分区内计算规则和分区间计算规则相同时，aggregateByKey 就可以简化为 foldByKey
 */
public class Spark18_RDD_Operator_Transform_FoldByKey {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建  PairRDD
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 60),
				new Tuple2<String, Integer>("class1", 60),
				new Tuple2<String, Integer>("class1", 60),
				new Tuple2<String, Integer>("class2", 60),
				new Tuple2<String, Integer>("class2", 50)
		);
		//平行化集合 生成JavaPairRDD  此处使用的是parallelizePairs
		JavaPairRDD<String, Integer> rdd = jsc.parallelizePairs(scoreList,2);

		//3.foldByKey :如果聚合计算时，分区内和分区间计算规则相同，spark提供了简化的方法
		JavaPairRDD<String, Integer> rdd1 = rdd.foldByKey(0,Integer::sum);

		//4.执行任务
		List<Tuple2<String, Integer>> collect = rdd1.collect();

		//5.输出结果
		collect.forEach(x -> System.out.println("key = " + x._1 + "  value = " + x._2));

		//6.关闭资源
		jsc.close();
	}

}
