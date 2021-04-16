package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子    reduceByKey
 * <p>
 * 可以将数据按照相同的 Key 对 Value 进行聚合
 * 分区内  和  分区间  计算规则是相同的
 */
public class Spark15_RDD_Operator_Transform_ReduceByKey {

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

		//3.reduceByKey: 相同的key的数据进行value数据的聚合操作
		//  reduceByKey中如果key的数据只有一个，是不会参与运算的。
		JavaPairRDD<String, Integer> rdd1 = rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer integer, Integer integer2) throws Exception {
				return integer + integer2;
			}
		});

		//4.执行任务
		List<Tuple2<String, Integer>> collect = rdd1.collect();

		//5.输出结果
		for (Tuple2<String, Integer> tuple2 : collect) {
			System.out.println("tuple2 = " + tuple2._1 + "   tuple2=" + tuple2._2);
		}

		//6.关闭资源
		jsc.close();
	}

}
