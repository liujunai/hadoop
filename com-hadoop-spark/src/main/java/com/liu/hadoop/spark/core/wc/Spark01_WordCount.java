package com.liu.hadoop.spark.core.wc;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @author LiuJunFeng
 * @date 2021/4/15 下午8:57
 * @description:   示例
 */
public class Spark01_WordCount {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> rdd = jsc.textFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/spark.txt");

		JavaRDD<String> map = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

		JavaPairRDD<String, Iterable<String>> groupBy = map.groupBy(x -> x);

		JavaPairRDD<String, Integer> mapValues = groupBy.mapValues(Iterables::size);

		mapValues.collect().forEach(x -> System.out.println("key = " + x._1() + "   value = " + x._2()));

		jsc.close();
	}

}
