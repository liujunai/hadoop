package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子    aggregateByKey
 * <p>
 * 将数据根据不同的规则进行分区内计算和分区间计算
 */
public class Spark17_RDD_Operator_Transform_AggregateByKey {

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

		//3.aggregateByKey :存在函数柯里化，有两个参数列表
		//          	第一个参数列表,需要传递一个参数，表示为初始值  决定返回值
		//              	主要用于当碰见第一个key的时候，和value进行分区内计算
		//         		第二个参数列表需要传递2个参数
		//           		第一个参数表示分区内计算规则
		//              	第二个参数表示分区间计算规则
		JavaPairRDD<String, Integer> rdd1 = rdd.aggregateByKey(0, Math::max, Integer::sum);

		//4.执行任务
		List<Tuple2<String, Integer>> collect = rdd1.collect();

		//5.输出结果
		collect.forEach(x -> System.out.println("key = " + x._1 + "  value = " + x._2));

		//6.关闭资源
		jsc.close();
	}

}
