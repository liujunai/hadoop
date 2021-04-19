package com.liu.hadoop.spark.sql.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;


/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午1:07
 * @description: spark 连接 hive  demo  数据装备
 */
public class Spark05_SparkSQL_Hive_Demo {

	public static void main(String[] args) throws AnalysisException {

		// 本机用户名 和 Hadoop 配置的访问 用户名不一致 会有权限错误
		// 修改访问用户名
//		System.setProperty("HADOOP_USER_NAME", "liu");

		// 创建SparkSQL的运行环境 SparkSession
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql");
		SparkSession spark = SparkSession.builder()
				.enableHiveSupport() //启用Hive的支持
				.config("hive.metastore.uris", "thrift://192.168.122.51:9083") // 要连接的 Hive 启动元数据服务
				// 在开发工具中创建数据库默认是在本地仓库，通过参数修改数据库仓库的地址:
				.config("spark.sql.warehouse.dir", "hdfs://liu1:8020/user/hive/warehouse")
				.config(conf).getOrCreate();

		// 选择使用的库
		spark.sql("use liu");

		spark.sql("CREATE TABLE `user_visit_action`(\n" +
				" `date` string,\n" +
				" `user_id` bigint,\n" +
				" `session_id` string,\n" +
				" `page_id` bigint,\n" +
				" `action_time` string,\n" +
				" `search_keyword` string,\n" +
				" `click_category_id` bigint,\n" +
				" `click_product_id` bigint,\n" +
				" `order_category_ids` string,\n" +
				" `order_product_ids` string,\n" +
				" `pay_category_ids` string,\n" +
				" `pay_product_ids` string,\n" +
				" `city_id` bigint)\n" +
				"row format delimited fields terminated by ','");

		spark.sql("load data local inpath '/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/user_visit_action1.txt'" +
				" overwrite into table liu.user_visit_action");

		spark.sql("CREATE TABLE `product_info`(\n" +
				" `product_id` bigint,\n" +
				" `product_name` string,\n" +
				" `extend_info` string)\n" +
				"row format delimited fields terminated by ','");

		spark.sql("load data local inpath '/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/product_info.txt' " +
				"overwrite into table liu.product_info");

		// 创建表
		spark.sql("CREATE TABLE if not exists `city_info`(\n" +
				" `city_id` bigint,\n" +
				" `city_name` string,\n" +
				" `area` string)\n" +
				"row format delimited fields terminated by ','");
		// 将本地文件加载到hive中
		spark.sql("load data local inpath '/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/city_info.txt'" +
				" overwrite into table liu.city_info");
		// 查看结果
		spark.sql("select * from city_info;").show();

		spark.close();
	}

}
