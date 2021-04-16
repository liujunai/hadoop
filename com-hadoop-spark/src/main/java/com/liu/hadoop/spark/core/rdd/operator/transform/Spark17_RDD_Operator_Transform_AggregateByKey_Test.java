package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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
public class Spark17_RDD_Operator_Transform_AggregateByKey_Test {

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
		JavaPairRDD<String, Integer> rdd = jsc.parallelizePairs(scoreList, 2);


		//3.aggregateByKey 最终的返回数据结果应该和初始值的类型保持一致
		JavaPairRDD<String, Tuple2<Integer, Integer>> rdd11 = rdd.aggregateByKey(new Tuple2<Integer, Integer>(0, 0),
				(x, y) -> new Tuple2(x._1() + y, x._2() + 1),
				(x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2()));


//		JavaPairRDD<String, Tuple2<Integer, Integer>> rdd1 = rdd.aggregateByKey(new Tuple2<Integer, Integer>(0, 0), new Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>() {
//			@Override
//			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Integer v2) throws Exception {
//				return new Tuple2<>(v1._1() + v2, v1._2() + 1);
//			}
//		}, new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
//			@Override
//			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
//				return new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2());
//			}
//		});

		JavaPairRDD<String, Integer> rdd22 = rdd11.mapValues(x -> x._1() / x._2());

//		JavaPairRDD<String, Integer> rdd2 = rdd1.mapValues(new Function<Tuple2<Integer, Integer>, Integer>() {
//			@Override
//			public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
//				return v1._1() / v1._2();
//			}
//		});


		//4.执行任务
		List<Tuple2<String, Integer>> collect = rdd22.collect();

		//5.输出结果
		collect.forEach(x -> System.out.println("key = " + x._1() + "  value = " + x._2()));

		//6.关闭资源
		jsc.close();
	}

}
