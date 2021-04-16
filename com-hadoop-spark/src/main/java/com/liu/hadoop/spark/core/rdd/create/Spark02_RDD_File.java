package com.liu.hadoop.spark.core.rdd.create;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/13 下午7:45
 * @description:  从外部存储（文件）创建 RDD
 */
public class Spark02_RDD_File {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从文件中创建RDD，将文件中的数据作为处理的数据源
		// textFile : 以行为单位来读取数据，读取的数据都是字符串
		// path路径默认以当前环境的根路径为基准。可以写绝对路径，也可以写相对路径
		// path路径可以是文件的具体路径，也可以目录名称
		// path路径还可以使用通配符 *
		// path还可以是分布式存储系统路径：HDFS
		JavaRDD<String> stringJavaRDD = jsc.textFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/11.txt");

		//  textFile可以将文件作为数据处理的数据源，默认也可以设定分区。
		//     minPartitions : 最小分区数量
		//     math.min(defaultParallelism, 2)
		//     val rdd = sc.textFile("datas/11.txt")
		// 如果不想使用默认的分区数量，可以通过第二个参数指定分区数
		// Spark读取文件，底层其实使用的就是Hadoop的读取方式

		// 1. 数据以行为单位进行读取
		//    spark读取文件，采用的是hadoop的方式读取，所以一行一行读取，和字节数没有关系
		// 2. 数据读取时以偏移量为单位,偏移量不会被重复读取
		// 3. 数据分区的偏移量范围的计算
		// 4. 如果数据源为多个文件，那么计算分区时以文件为单位进行分区


		//3.执行任务
		List<String> collect = stringJavaRDD.collect();

		//4.输出结果
		for (String str : collect) {
			System.out.println("str = " + str);
		}

		//5.关闭资源
		jsc.close();

	}


}
