package com.liu.hadoop.spark.core.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/13 下午7:45
 * @description:   RDD  统计案例
 */
public class Spark01_RDD_Request {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从文件中创建RDD，将文件中的数据作为处理的数据源
		JavaRDD<String> rdd = jsc.textFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/user_visit_action.txt");



		//3.执行任务
		List<String> collect = rdd.collect();

		//4.输出结果
		for (String str : collect) {
			System.out.println("str = " + str);
		}

		//5.关闭资源
		jsc.close();

	}


}
