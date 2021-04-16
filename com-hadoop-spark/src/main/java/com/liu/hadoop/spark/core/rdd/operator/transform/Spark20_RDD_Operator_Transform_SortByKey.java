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
 * @description: RDD 转换算子    sortByKey
 * <p>
 * 在一个(K,V)的 RDD 上调用，K 必须实现 Ordered 接口(特质)，返回一个按照 key 进行排序的
 *
 */
public class Spark20_RDD_Operator_Transform_SortByKey {

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

		//3.sortByKey :方法需要三个参数
		// 第一个参数 自定义比较器
		// 第二个参数是ascending，排序后RDD中的Key是升序还是降序，默认是true，也就是升序；
		// 第三个参数是numPartitions，该参数决定排序后的RDD的分区个数，默认排序后的分区个数和排序之前的个数相等，即为this.partitions.size。

		JavaPairRDD<String, Integer> rdd1 = rdd.sortByKey(false);


		//4.执行任务
		List<Tuple2<String, Integer>> collect = rdd1.collect();

		//5.输出结果
		collect.forEach(x -> System.out.println("key = " + x._1() + "  value = " + x._2()));

		//6.关闭资源
		jsc.close();
	}

}
