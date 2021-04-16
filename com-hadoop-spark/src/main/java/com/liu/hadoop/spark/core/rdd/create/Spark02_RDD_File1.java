package com.liu.hadoop.spark.core.rdd.create;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/13 下午7:45
 * @description:  从外部存储（文件）创建 RDD
 */
public class Spark02_RDD_File1 {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从文件中创建RDD，将文件中的数据作为处理的数据源
		// wholeTextFiles : 以文件为单位读取数据  读取的结果表示为元组，第一个元素表示文件路径，第二个元素表示文件内容
		// path路径默认以当前环境的根路径为基准。可以写绝对路径，也可以写相对路径
		// path路径可以是文件的具体路径，也可以目录名称
		// path路径还可以使用通配符 *
		// path还可以是分布式存储系统路径：HDFS
		JavaPairRDD<String, String> stringStringJavaPairRDD = jsc.wholeTextFiles("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/*1.txt");


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
