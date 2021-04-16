package com.liu.hadoop.spark.core.rdd.persistence;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/13 下午7:45
 * @description: RDD 持久化 : RDD中不存储数据,如果一个RDD需要重复使用，只需要（血缘依赖）从头再次执行来获取数据, RDD对象可以重用，但是数据无法重用
 * <p>
 * RDD Cache 缓存
 * RDD 通过 Cache 或者 Persist 方法将前面的计算结果缓存，默认情况下会把数据以缓存
 * 在 JVM 的堆内存中。但是并不是这两个方法被调用时立即缓存，而是触发后面的 action 算
 * 子时，该 RDD 将会被缓存在计算节点的内存中，并供后面重用。
 */
public class Spark01_RDD_Persistence {

	public static void main(String[] args) {


		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		List<String> list = Arrays.asList("hello hadoop", "hello spark");
		JavaRDD<String> rdd = jsc.parallelize(list);

		JavaRDD<String> words = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());

		JavaPairRDD<String, Integer> wordOne = words.mapToPair((PairFunction<String, String, Integer>) s -> {
			System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1");
			return new Tuple2<>(s, 1);
		});

		// cache 是默认持久化的操作，只能将数据保存到内存中，如果想要保存到磁盘文件，需要更改存储级别
		// cache 操作会增加血缘关系，不改变原有的血缘关系
		wordOne.cache();

		// persist 更改持久化级别
		// 持久化操作必须在行动算子执行时完成的。
//		wordOne.persist(StorageLevel.DISK_ONLY());

		JavaPairRDD<String, Integer> wordCount = wordOne.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);
		wordCount.collect().forEach(x -> System.out.println("x = " + x));

		System.out.println("-------------------------------------------------------------------------------------");

		JavaPairRDD<String, Iterable<Integer>> wordCount1 = wordOne.groupByKey();
		wordCount1.collect().forEach(x -> System.out.println("x = " + x));


		jsc.close();

	}


}
