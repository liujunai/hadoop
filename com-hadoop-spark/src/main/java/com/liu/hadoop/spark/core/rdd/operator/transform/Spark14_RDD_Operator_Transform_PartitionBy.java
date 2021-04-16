package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子    partitionBy
 * <p>
 * 将数据按照指定 Partitioner 重新进行分区。Spark 默认的分区器是 HashPartitioner
 */
public class Spark14_RDD_Operator_Transform_PartitionBy {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list);

		//3.map 转换格式
		JavaRDD<Tuple2<Integer, Integer>> rdd1 = rdd.map(integer -> new Tuple2<>(integer, 1));

		// 将RDD 转成 pairRDD( Key - Value 类型 )
		JavaPairRDD<Integer, Integer> pairRDD = rdd1.mapToPair(pair -> new Tuple2<Integer, Integer>(pair._1(), pair._2()));

		// partitionBy根据指定的分区规则对数据进行重分区
		JavaPairRDD<Integer, Integer> partitionBy = pairRDD.partitionBy(new HashPartitioner(2));

		partitionBy.saveAsTextFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/output");

		//6.关闭资源
		jsc.close();
	}

}
