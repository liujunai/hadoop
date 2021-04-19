package com.liu.hadoop.spark.sql.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午1:07
 * @description: spark 连接  hbase  读取 / 写入
 */
public class Spark06_SparkSQL_Hbase {

	public static void main(String[] args) throws AnalysisException {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql");
		SparkSession spark = SparkSession.builder()
				.enableHiveSupport() //启用Hive的支持
				.config("hive.metastore.uris", "thrift://192.168.122.51:9083") // 要连接的 Hive 启动元数据服务
				// 在开发工具中创建数据库默认是在本地仓库，通过参数修改数据库仓库的地址:
				.config("spark.sql.warehouse.dir", "hdfs://liu1:8020/user/hive/warehouse")
				.config(conf).getOrCreate();

		// 选择使用的库
		spark.sql("use liu");


		spark.close();
	}

}
