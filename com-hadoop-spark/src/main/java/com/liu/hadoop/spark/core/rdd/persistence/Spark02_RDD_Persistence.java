package com.liu.hadoop.spark.core.rdd.persistence;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/13 下午7:45
 * @description: RDD 持久化 : RDD中不存储数据,如果一个RDD需要重复使用，只需要（血缘依赖）从头再次执行来获取数据, RDD对象可以重用，但是数据无法重用
 * <p>
 * RDD CheckPoint 检查点
 * 所谓的检查点其实就是通过将 RDD 中间结果写入磁盘
 * 由于血缘依赖过长会造成容错成本过高，这样就不如在中间阶段做检查点容错，如果检查点
 * 之后有节点出现问题，可以从检查点开始重做血缘，减少了开销。
 * 对 RDD 进行 checkpoint 操作并不会马上被执行，必须执行 Action 操作才能触发
 */
public class Spark02_RDD_Persistence {

	public static void main(String[] args) {


		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		// checkpoint 检查点路径
		jsc.setCheckpointDir("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/checkpoint");

		List<String> list = Arrays.asList("hello hadoop", "hello spark");
		JavaRDD<String> rdd = jsc.parallelize(list);

		JavaRDD<String> words = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());

		JavaPairRDD<String, Integer> wordOne = words.mapToPair((PairFunction<String, String, Integer>) s -> {
			System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1");
			return new Tuple2<>(s, 1);
		});

		// checkpoint 需要落盘，需要指定检查点保存路径
		// 检查点路径保存的文件，当作业执行完毕后，不会被删除
		// 一般保存路径都是在分布式存储系统：HDFS
		wordOne.checkpoint();

		JavaPairRDD<String, Integer> wordCount = wordOne.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);
		wordCount.collect().forEach(x -> System.out.println("x = " + x));

		System.out.println("-------------------------------------------------------------------------------------");

		JavaPairRDD<String, Iterable<Integer>> wordCount1 = wordOne.groupByKey();
		wordCount1.collect().forEach(x -> System.out.println("x = " + x));

		jsc.close();

	}


}
