package com.liu.hadoop.spark.sql.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午1:07
 * @description: spark 连接 hive  demo  查询示例
 */
public class Spark05_SparkSQL_Hive_Demo1 {

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

		Dataset<Row> sql = spark.sql("select\n" +
				"  *\n" +
				"from (\n" +
				"    select\n" +
				"        *,\n" +
				"        rank() over( partition by area order by clickCnt desc ) as rank\n" +
				"    from (\n" +
				"        select\n" +
				"            area,\n" +
				"            product_name,\n" +
				"            count(*) as clickCnt\n" +
				"        from (\n" +
				"            select\n" +
				"                a.*,\n" +
				"                p.product_name,\n" +
				"                c.area,\n" +
				"                c.city_name\n" +
				"            from user_visit_action a\n" +
				"            join product_info p on a.click_product_id = p.product_id\n" +
				"            join city_info c on a.city_id = c.city_id\n" +
				"            where a.click_product_id > -1\n" +
				"        ) t1 group by area, product_name\n" +
				"    ) t2\n" +
				") t3 where rank <= 3");

		sql.show();

		spark.close();
	}

}
