package com.liu.hadoop.spark.sql.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午1:07
 * @description:  数据的加载和保存
 * SparkSQL 提供了通用的保存数据和数据加载的方式。这里的通用指的是使用相同的API，
 * 根据不同的参数读取和保存不同格式的数据，SparkSQL 默认读取和保存的文件格式为 parquet
 *
 * parquet: Spark SQL 的默认数据源为 Parquet 格式。Parquet 是一种能够有效存储嵌套数据的列式存储格式
 * 			修改配置项 spark.sql.sources.default，可修改默认数据源格式
 *
 * json:	Spark SQL 能够自动推测 JSON 数据集的结构，并将它加载为一个 Dataset[Row]
 * 			注意：Spark 读取的 JSON 文件不是传统的 JSON 文件，每一行都应该是一个 JSON 串 (每一行都要符合JSON格式)
 *
 * csv:		Spark SQL 可以配置 CSV 文件的列表信息，读取 CSV 文件,CSV 文件的第一行设置为数据列
 *			spark.read().format("csv").option("sep", ";").option("inferSchema", "true").option("header", "true").load("路径")
 *											文件内容的分隔符		 	是否进行类型推断				是否保留表头
 *
 */
public class Spark03_SparkSQL_FIle {

	public static void main(String[] args) {

		// 创建SparkSQL的运行环境 SparkSession
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// spark.read()
		// csv format jdbc json load option options orc parquet schema table text textFile

		//1) 加载数据
		// spark.read.load 是加载数据的通用方法
		// option("…")：在"jdbc"格式下需要传入 JDBC 相应参数，url、user、password 和 dbtable

		// spark.read().format("json").load("路径")
		// format("…")：指定加载的数据类型，包括"csv"、"jdbc"、"json"、"orc"、"parquet"和"textFile"。
		// load("…")：在"csv"、"jdbc"、"json"、"orc"、"parquet"和"textFile"格式下需要传入加载数据的路径。

		// 也可以直接 spark.read().json("路径")
		Dataset<Row> rddJson = spark.read().format("json").load("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/test.json");
//----------------------------------------------------------------------------------------------------------------------

		// rddJson.write()
		// csv jdbc json orc parquet textFile … …

		// 2) 保存数据
		// df.write.save 是保存数据的通用方法
		// option("…")：在"jdbc"格式下需要传入 JDBC 相应参数，url、user、password 和 dbtable

		// rddJson.write().format("json").save("路径");
		// format("…")：指定保存的数据类型，包括"csv"、"jdbc"、"json"、"orc"、"parquet"和"textFile"。
		// save ("…")：在"csv"、"orc"、"parquet"和"textFile"格式下需要传入保存数据的路径。

		// 也可以 rddJson.write().json("路径");
		rddJson.write().format("json").save("路径");

//----------------------------------------------------------------------------------------------------------------------

		// 3. 保存模式
		// 保存操作可以使用 SaveMode, 用来指明如何处理数据，使用 mode()方法来设置。
		// 有一点很重要: 这些 SaveMode 都是没有加锁的, 也不是原子操作。
		// SaveMode 是一个枚举类，其中的常量包括：
		// Scala/Java								 Any Language 						Meaning
		// SaveMode.ErrorIfExists(default) 			"error"(default) 					如果文件已经存在则抛出异常
		// SaveMode.Append 							"append" 							如果文件已经存在则追加
		// SaveMode.Overwrite 						"overwrite" 						如果文件已经存在则覆盖
		// SaveMode.Ignore 							"ignore"							如果文件已经存在则忽略

		rddJson.write().format("json").mode("append").save("");



		spark.close();


	}

}
