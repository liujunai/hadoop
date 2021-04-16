package com.liu.hadoop.spark.core.rdd.partitions;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/16 上午3:55
 * @description: RDD 分区器
 * <p>
 * Spark 目前支持 Hash 分区和 Range 分区，和用户自定义分区。Hash 分区为当前的默认
 * 分区。分区器直接决定了 RDD 中分区的个数、RDD 中每条数据经过 Shuffle 后进入哪个分
 * 区，进而决定了 Reduce 的个数。
 * ➢ 只有 Key-Value 类型的 RDD 才有分区器，非 Key-Value 类型的 RDD 分区的值是 None
 * ➢ 每个 RDD 的分区 ID 范围：0 ~ (numPartitions - 1)，决定这个值是属于那个分区的。
 * <p>
 * Hash 分区：对于给定的 key，计算其 hashCode,并除以分区个数取余
 * <p>
 * Range 分区：将一定范围内的数据映射到一个分区中，尽量保证每个分区数据均匀，而且分区间有序
 */
public class Spark01_RDD_Partitions {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Tuple2<String, String>> list = Arrays.asList(
				new Tuple2<>("人族", "剑侠客"),
				new Tuple2<>("魔族", "虎头怪"),
				new Tuple2<>("仙族", "舞天姬"),
				new Tuple2<>("人族", "飞燕女"),
				new Tuple2<>("魔族", "狐美人"),
				new Tuple2<>("仙族", "神天兵")
		);
		JavaPairRDD<String, String> rdd = jsc.parallelizePairs(list);

		JavaPairRDD<String, String> rdd1 = rdd.partitionBy(new MyPartitions());

		rdd1.saveAsTextFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/output");


		jsc.close();
	}

}
