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
 * @description:    RDD 血缘关系
 *
 * RDD 只支持粗粒度转换，即在大量记录上执行的单个操作。将创建 RDD 的一系列 Lineage
 * （血统）记录下来，以便恢复丢失的分区。RDD 的 Lineage 会记录 RDD 的元数据信息和转
 * 换行为，当该 RDD 的部分分区数据丢失时，它可以根据这些信息来重新运算和恢复丢失的
 * 数据分区
 *
 * 相邻的两个RDD的关系称之为依赖关系
 * 新的RDD依赖于旧的RDD
 * 多个连续的RDD的依赖关系，称之为血缘关系
 *
 * RDD不会保存数据的
 * RDD为了提供容错性，需要将RDD的关系保存下来
 * 一旦出现错误，可以根据血缘关系从数据源重新读取进行计算
 */
public class Spark01_RDD_Dependency {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> rdd = jsc.textFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/spark.txt");
		System.out.println("rdd = " + rdd.toDebugString());
		System.out.println("-------------------------------------- rdd --------------------------------------");

		JavaRDD<String> map = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		System.out.println("map = " + map.toDebugString());
		System.out.println("-------------------------------------- map --------------------------------------");

		JavaPairRDD<String, Iterable<String>> groupBy = map.groupBy(x -> x);
		System.out.println("groupBy = " + groupBy.toDebugString());
		System.out.println("-------------------------------------- groupBy --------------------------------------");

		JavaPairRDD<String, Integer> mapValues = groupBy.mapValues(Iterables::size);
		System.out.println("mapValues = " + mapValues.toDebugString());
		System.out.println("-------------------------------------- mapValues --------------------------------------");

		mapValues.collect().forEach(x -> System.out.println("key = " + x._1() + "   value = " + x._2()));

		jsc.close();
	}
}
