package com.liu.hadoop.spark.sql.basic;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午1:05
 * @description: DataFrame  是一种以 RDD 为基础的分布式数据集 类似于传统数据库中的二维表格
 * <p>
 * DataFrame 与 RDD 的主要区别在于，DataFrame 带有 schema 元信息，即 DataFrame
 * 所表示的二维表数据集的每一列都带有名称和类型。这使得 Spark SQL 得以洞察更多的结构
 * 信息，从而对藏于 DataFrame 背后的数据源以及作用于 DataFrame 之上的变换进行了针对性
 * 的优化，最终达到大幅提升运行时效率的目标。反观 RDD，由于无从得知所存数据元素的
 * 具体内部结构，Spark Core 只能在 stage 层面进行简单、通用的流水线优化。
 */
public class Spark01_SparkSQL_DataFrame {

	public static void main(String[] args) throws AnalysisException {

		// 创建SparkSQL的运行环境 SparkSession
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
		spark.implicits();

		// 从文件获取数据源
//		Dataset<String> stringDataset = spark.read().textFile("");
//		Dataset<Row> csv = spark.read().csv("");
		Dataset<Row> jsonRDD = spark.read().json("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/test.json");
		// 显示内容 指定条数
//		jsonRDD.show(2);

		// DataFrame 的 SQL 操作
		// 创建临时视图
		jsonRDD.createTempView("user");
//		spark.sql("select id,name,age from user").show();
//		spark.sql("select id,name,age from user where id = '3'").show();
		spark.sql("select avg(age) as age from user ").show();


		// DataFrame 的 DSL 操作
//		jsonRDD.select(col("name").as("NAME"),col("age")).show();



		spark.close();



	}

}
