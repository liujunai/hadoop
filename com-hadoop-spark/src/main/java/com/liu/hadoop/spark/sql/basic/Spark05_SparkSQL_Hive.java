package com.liu.hadoop.spark.sql.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;


/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午1:07
 * @description: spark 连接 hive  读取 / 存储
 * <p>
 * Spark SQL 可以通过 JDBC 从关系型数据库中读取数据的方式创建 DataFrame
 */
public class Spark05_SparkSQL_Hive {

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
//				.config("spark.sql.warehouse.dir", "hdfs://liu1:8020/user/hive/warehouse")
				.config(conf).getOrCreate();

		// 使用SparkSQL连接外置的Hive
		// 1. 拷贝Hive-size.xml文件到classpath下
		// 2. 启用Hive的支持  enableHiveSupport
		// 3. 增加对应的依赖关系（包含MySQL驱动）
		spark.sql("use liu");
//		spark.sql("show tables").show();


//----------------------------------------------------------------------------------------------------------------------
		//spark 连接 hive   存储

		// 读取CSV文件加载数据
		Dataset<Row> load = spark.read().format("csv")
				.option("sep", ",")
				.option("header", true)
				.load("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/liu.csv");

		// 过滤数据
		Dataset<Row> createDate = load.select(to_date(col("TransTime")).as("TransTime"),
				col("GantryId"), col("MediaType"),
				col("PayFee").cast(DataTypes.IntegerType),
				col("SpecialType")).where(month(col("TransTime")).isin(2)).repartition(1);

		// 创建临时视图
		createDate.createTempView("tmp");

		// 通过查询语句向表中插入数据
		spark.sql("insert into liu.ods_gantry_transaction partition(month='202102') " +
				"select GantryId,MediaType,TransTime,PayFee,SpecialType " +
				"from tmp ");

//----------------------------------------------------------------------------------------------------------------------

		// spark 连接 hive 读取
//		Dataset<Row> sql = spark.sql("select * from ods_gantry_transaction where month='202102'");
//		sql.show();



		spark.close();
	}

}
