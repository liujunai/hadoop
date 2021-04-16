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
 * @description: RDD 转换算子    join
 * <p>
 * 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素连接在一起的
 *
 */
public class Spark21_RDD_Operator_Transform_Join {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建  PairRDD
		List<Tuple2<String, Integer>> scoreList1 = Arrays.asList(
				new Tuple2<String, Integer>("class1", 1),
				new Tuple2<String, Integer>("class2", 5),
				new Tuple2<String, Integer>("class1", 2),
				new Tuple2<String, Integer>("class2", 6)
		);
		//平行化集合 生成JavaPairRDD  此处使用的是parallelizePairs
		JavaPairRDD<String, Integer> rdd1 = jsc.parallelizePairs(scoreList1);

		List<Tuple2<String, Integer>> scoreList2 = Arrays.asList(
				new Tuple2<String, Integer>("class1", 3),
				new Tuple2<String, Integer>("class2", 7),
				new Tuple2<String, Integer>("class1", 4),
				new Tuple2<String, Integer>("class2", 8)
		);
		JavaPairRDD<String, Integer> rdd2 = jsc.parallelizePairs(scoreList2);

		//3.join: 两个不同数据源的数据，相同的key的value会连接在一起，形成元组
		//        如果两个数据源中key没有匹配上，那么数据不会出现在结果中
		//        如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔乘积，数据量会几何性增长，会导致性能降低。
		JavaPairRDD<String, Tuple2<Integer, Integer>> join = rdd1.join(rdd2);


		//4.执行任务
		List<Tuple2<String, Tuple2<Integer, Integer>>> collect = join.collect();

		//5.输出结果
		collect.forEach(x -> System.out.println("key = " + x._1() + "  value = " + x._2()));

		//6.关闭资源
		jsc.close();
	}

}
