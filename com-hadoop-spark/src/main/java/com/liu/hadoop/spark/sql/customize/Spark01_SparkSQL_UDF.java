package com.liu.hadoop.spark.sql.customize;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;

import java.math.BigDecimal;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午1:05
 * @description:   自定义函数 UDF
 */
public class Spark01_SparkSQL_UDF {

	public static void main(String[] args) throws AnalysisException {

		// 创建SparkSQL的运行环境 SparkSession
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		Dataset<Row> rdd = spark.read().json("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/test.json");

		// 创建视图
		rdd.createTempView("user");


		// 此时注册的方法 只能在sql()中可见，对DataFrame API不可见
		// spark.udf().register(函数名,函数体,函数输出类型);
		spark.udf().register("addq", new UDF1<String, String>() {
			@Override
			public String call(String s) throws Exception {
				return "前缀:"+s;
			}
		}, DataTypes.StringType);

		spark.sql("select id,addq(name),age from user").show();


		spark.close();



	}

}
