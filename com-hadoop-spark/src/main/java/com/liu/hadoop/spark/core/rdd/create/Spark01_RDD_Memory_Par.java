package com.liu.hadoop.spark.core.rdd.create;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/13 下午7:45
 * @description:  从集合（内存）中创建 K - V   RDD
 */
public class Spark01_RDD_Memory_Par {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
//		sparkConf.set("spark.default.parallelism", "5");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD，将内存中集合的数据作为处理的数据源
		// RDD的并行度 & 分区
		// parallelize 方法可以传递第二个参数，这个参数表示分区的数量
		// 第二个参数可以不传递的，那么makeRDD方法会使用默认值 ： default.Parallelism（默认并行度）
		//    scheduler.conf.getInt("spark.default.parallelism", totalCores)
		//    spark在默认情况下，从配置对象中获取配置参数：spark.default.parallelism
		//    如果获取不到，那么使用totalCores属性，这个属性取值为当前运行环境的最大可用核数
		List<Tuple2<String,Integer>> list = Arrays.asList(
				new Tuple2<>("aa",1),
				new Tuple2<>("bb",2),
				new Tuple2<>("cc",3),
				new Tuple2<>("dd",4)
		);
		JavaPairRDD<String, Integer> rdd = jsc.parallelizePairs(list, 3);

		//3.执行任务 (目录要不存在)
		rdd.saveAsTextFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/output");

		//5.关闭资源
		jsc.close();

	}


}
