package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子    leftOuterJoin  And  rightOuterJoin
 * <p>
 * leftOuterJoin : 类似于 SQL 语句的左外连接
 * rightOuterJoin : 类似于 SQL 语句的右外连接
 *
 */
public class Spark22_RDD_Operator_Transform_LRJoin {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建  PairRDD
		List<Tuple2<String, Integer>> scoreList1 = Arrays.asList(
//				new Tuple2<String, Integer>("class1", 1),
				new Tuple2<String, Integer>("class2", 2),
				new Tuple2<String, Integer>("class3", 3)
		);
		//平行化集合 生成JavaPairRDD  此处使用的是parallelizePairs
		JavaPairRDD<String, Integer> rdd1 = jsc.parallelizePairs(scoreList1);

		List<Tuple2<String, Integer>> scoreList2 = Arrays.asList(
				new Tuple2<String, Integer>("class1", 4),
				new Tuple2<String, Integer>("class2", 5),
				new Tuple2<String, Integer>("class3", 6)
		);
		JavaPairRDD<String, Integer> rdd2 = jsc.parallelizePairs(scoreList2);

		//3.leftOuterJoin:
		JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> leftJoin = rdd1.leftOuterJoin(rdd2);
		//3.rightOuterJoin:
		JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> rightJoin = rdd1.rightOuterJoin(rdd2);


		//4.执行任务
		List<Tuple2<String, Tuple2<Integer, Optional<Integer>>>> collect = leftJoin.collect();
		List<Tuple2<String, Tuple2<Optional<Integer>, Integer>>> collect1 = rightJoin.collect();

		//5.输出结果
		collect.forEach(x -> System.out.println("key = " + x._1() + "  value = " + x._2()));
		System.out.println("----------------------------------------");
		collect1.forEach(x -> System.out.println("key = " + x._1() + "  value = " + x._2()));

		//6.关闭资源
		jsc.close();
	}

}
