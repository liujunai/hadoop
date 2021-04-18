package com.liu.hadoop.spark.sql.customize;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import scala.reflect.api.TypeTags;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午1:05
 * @description: 自定义函数 UDAF
 * <p>
 *
 * 从 Spark3.0 版本后，UserDefinedAggregateFunction 已经不推荐使用了。可以统一采用  Aggregator 强类型聚合函数
 */
public class Spark02_SparkSQL_UDAF1 {

	public static void main(String[] args) throws AnalysisException {

		// 创建SparkSQL的运行环境 SparkSession
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		Dataset<Row> rdd = spark.read().json("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/test.json");

		// 创建视图
		rdd.createTempView("user");


		// 此时注册的方法 只能在sql()中可见，对DataFrame API不可见
		// spark.udf().register(函数名,functions.udaf(函数体,函数输入类型));
		spark.udf().register("ageAvg1", functions.udaf(new MyUDAF1(),Encoders.DOUBLE()));


		spark.sql("select ageAvg1(age) as age from user").show();


		spark.close();


	}

}
