package com.liu.hadoop.spark.core.rdd.dependency;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @author LiuJunFeng
 * @date 2021/4/16 上午1:58
 * @description:      RDD 依赖关系
 *
 * 新的RDD的一个分区的数据依赖于旧的RDD一个分区的数据
 * 这个依赖称之为 OneToOne 依赖  也叫窄依赖
 *
 * RDD 窄依赖 : 窄依赖表示每一个父(上游)RDD 的 Partition 最多被子（下游）RDD 的一个 Partition 使用，
 * 窄依赖我们形象的比喻为独生子女。
 *
 *
 * 新的RDD的一个分区的数据依赖于旧的RDD多个分区的数据
 * 这个依赖称之为 Shuffle 依赖 也叫宽依赖
 *
 * 4) RDD 宽依赖 : 宽依赖表示同一个父（上游）RDD 的 Partition 被多个子（下游）RDD 的 Partition 依赖，会
 * 引起 Shuffle，总结：宽依赖我们形象的比喻为多生。
 */
public class Spark02_RDD_Dependency {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> rdd = jsc.textFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/spark.txt");
		System.out.println("rdd = " + rdd.rdd().dependencies());
		System.out.println("-------------------------------------- rdd --------------------------------------");

		JavaRDD<String> map = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		System.out.println("map = " + map.rdd().dependencies());
		System.out.println("-------------------------------------- map --------------------------------------");

		JavaPairRDD<String, Iterable<String>> groupBy = map.groupBy(x -> x);
		System.out.println("groupBy = " + groupBy.rdd().dependencies());
		System.out.println("-------------------------------------- groupBy --------------------------------------");

		JavaPairRDD<String, Integer> mapValues = groupBy.mapValues(Iterables::size);
		System.out.println("mapValues = " + mapValues.rdd().dependencies());
		System.out.println("-------------------------------------- mapValues --------------------------------------");

		mapValues.collect().forEach(x -> System.out.println("key = " + x._1() + "   value = " + x._2()));

		jsc.close();
	}
}
