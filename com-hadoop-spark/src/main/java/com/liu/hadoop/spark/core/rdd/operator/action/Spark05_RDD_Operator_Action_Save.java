package com.liu.hadoop.spark.core.rdd.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.internal.config.R;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author LiuJunFeng
 * @date 2021/4/15 下午6:15
 * @description: RDD 行动算子    save all
 * <p>
 * 将数据保存到不同格式的文件中
 */
public class Spark05_RDD_Operator_Action_Save {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建  PairRDD
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<>("class1", 90),
				new Tuple2<>("class2", 60),
				new Tuple2<>("class2", 60),
				new Tuple2<>("class1", 60),
				new Tuple2<>("class1", 60),
				new Tuple2<>("class2", 50)
		);
		JavaPairRDD<String, Integer> rdd = jsc.parallelizePairs(scoreList);

		//3.save
		// saveAsTextFile 保存成 Text 文件
		// 第一个参数：Path为保存的路径；
		// 第二个参数：codec为压缩编码格式；
		rdd.saveAsTextFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/output");

		// saveAsObjectFile 用于将RDD中的元素序列化成对象，存储到文件中。
		rdd.saveAsObjectFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/output1");

		// saveAsHadoopFile 是将RDD存储在HDFS上的文件中，支持老版本Hadoop API。
		// 可以指定outputKeyClass、outputValueClass以及压缩格式。
		// 每个分区输出一个文件。
//		rdd.saveAsHadoopFile();

		// saveAsNewAPIHadoopFile 用于将RDD数据保存到HDFS上，使用新版本Hadoop API。
		// 用法基本同 saveAsHadoopFile。
//		rdd.saveAsNewAPIHadoopFile();

		// saveAsHadoopDataset：
		// 使用旧的Hadoop API将RDD输出到任何Hadoop支持的存储系统，为该存储系统使用Hadoop JobConf 对象。
		// JobConf设置一个OutputFormat和任何需要的输出路径(如要写入的表名)，就像为Hadoop MapReduce作业配置的那样。
//		rdd.saveAsHadoopDataset();

		// saveAsNewAPIHadoopDataset：
		// 使用新的Hadoop API将RDD输出到任何Hadoop支持的存储系统，为该存储系统使用Hadoop Configuration对象。
		// Conf设置一个OutputFormat和任何需要的输出路径(如要写入的表名)，就像为Hadoop MapReduce作业配置的那样。
//		rdd.saveAsNewAPIHadoopDataset();

		jsc.stop();

	}

}
