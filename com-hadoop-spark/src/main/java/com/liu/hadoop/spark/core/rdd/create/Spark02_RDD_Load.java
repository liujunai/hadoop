package com.liu.hadoop.spark.core.rdd.create;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/13 下午7:45
 * @description:  从外部存储（文件）创建 RDD
 *
 * Spark 的数据读取及数据保存可以从两个维度来作区分：文件格式以及文件系统。
 * 文件格式分为：text 文件、csv 文件、sequence 文件以及 Object 文件；
 * 文件系统分为：本地文件系统、HDFS、HBASE 以及数据库
 *
 */
public class Spark02_RDD_Load {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从文件中创建RDD，将文件中的数据作为处理的数据源
		JavaPairRDD<String, String> stringStringJavaPairRDD = jsc.wholeTextFiles("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/*1.txt");

		//  读取文件  Spark05_RDD_Operator_Action_Save 中是 保存到文件系统
//		jsc.textFile("output1");
//		jsc.objectFile("output2");
//		jsc.hadoopFile("");
//		jsc.newAPIHadoopFile("");



		//3.执行任务
		List<Tuple2<String, String>> collect = stringStringJavaPairRDD.collect();

		//4.输出结果  string1 文件路径  string2 文件内容
		for (Tuple2<String, String> stringStringTuple2 : collect) {
			System.out.println("string1 = " + stringStringTuple2._1() + "\n" +
					"string2 = " + stringStringTuple2._2());
		}

		//5.关闭资源
		jsc.close();

	}


}
