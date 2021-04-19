package com.liu.hadoop.spark.sql.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Properties;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午1:07
 * @description: Spark 连接 JDBC 读取 / 存储
 * <p>
 * Spark SQL 可以通过 JDBC 从关系型数据库中读取数据的方式创建 DataFrame
 */
public class Spark04_SparkSQL_JDBC {

	public static void main(String[] args) {

		// 创建SparkSQL的运行环境 SparkSession
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// 读取MySQL数据
		Dataset<Row> rdd = spark.read().format("jdbc")
				.option("url", "jdbc:mysql://192.168.122.51:3306/spark?&useUnicode=true&characterEncoding=UTF-8")
				.option("driver", "com.mysql.jdbc.Driver")
				.option("user", "liu")
				.option("password", "a")
				.option("dbtable", "user")
				.load();

	/**
		// 方式 2:通用的 load 方法读取 参数另一种形式
		HashMap<String, String> map = new HashMap<>();
		map.put("url", "jdbc:mysql://192.168.122.51:3306/spark?user=liu&password=a&useUnicode=true&characterEncoding=UTF-8");
		map.put("driver", "com.mysql.jdbc.Driver");
		map.put("dbtable", "user");
		spark.read().format("jdbc")
				.options(map)
				.load()
				.show();

		//方式 3:使用 jdbc 方法读取
		String url = "jdbc:mysql://192.168.122.51:3306/spark?user=liu&password=a&useUnicode=true&characterEncoding=UTF-8";
		String table = "user";
		Properties props = new Properties();
		props.put("user", "liu");
		props.put("password", "a");
		props.put("driver", "com.mysql.jdbc.Driver");
		spark.read().jdbc(url, table, props).show();
	*/

		rdd.show();

		// 保存数据
		rdd.write().format("jdbc")
				.option("url", "jdbc:mysql://192.168.122.51:3306/spark?&useUnicode=true&characterEncoding=UTF-8")
				.option("driver", "com.mysql.jdbc.Driver")
				.option("user", "liu")
				.option("password", "a")
				.option("dbtable", "user1")
				.mode(SaveMode.Overwrite)
				.save();

	/**
		//方式 2：通过 jdbc 方法
		String url2 = "jdbc:mysql://192.168.122.51:3306/spark?user=liu&password=a&useUnicode=true&characterEncoding=UTF-8";
		String table2 = "user1";
		Properties props2 = new Properties();
		props2.setProperty("user", "root");
		props2.setProperty("password", "123123");
		rdd.write().mode(SaveMode.Append).jdbc(url2, table2, props2);
	*/

		spark.close();
	}

}
