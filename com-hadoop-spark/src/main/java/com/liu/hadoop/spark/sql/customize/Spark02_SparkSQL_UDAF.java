package com.liu.hadoop.spark.sql.customize;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午1:05
 * @description: 自定义函数 UDAF
 * <p>
 *
 * 通过继承 UserDefinedAggregateFunction 来实现用户自定义弱类型聚合函数
 */
public class Spark02_SparkSQL_UDAF {

	public static void main(String[] args) throws AnalysisException {

		// 创建SparkSQL的运行环境 SparkSession
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		Dataset<Row> rdd = spark.read().json("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/test.json");

		// 创建视图
		rdd.createTempView("user");


		// 此时注册的方法 只能在sql()中可见，对DataFrame API不可见
		// spark.udf().register(函数名,函数体,函数输出类型);
		spark.udf().register("ageAvg", new MyUDAF());


		spark.sql("select ageAvg(age) as age from user").show();


		spark.close();


	}

}
