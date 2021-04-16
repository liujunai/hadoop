package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子      flatMap
 * 
 * 将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射
 */
public class Spark04_RDD_Operator_Transform_flatMap {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从文件中创建RDD
		JavaRDD<String> rdd = jsc.textFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/spark.txt");

		//3.flatMap
		JavaRDD<String> rdd1 = rdd.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) throws Exception {
				return Arrays.asList(s.split(" ")).iterator();
			}
		});

		//4.执行任务
		List<String> collect = rdd1.collect();

		System.out.println(collect);
		//5.输出结果
		for (String str : collect) {
			System.out.println(str);
		}

		//6.关闭资源
		jsc.close();
	}

}
