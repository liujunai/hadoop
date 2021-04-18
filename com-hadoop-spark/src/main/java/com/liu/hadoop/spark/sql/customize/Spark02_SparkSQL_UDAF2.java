package com.liu.hadoop.spark.sql.customize;

import com.liu.hadoop.spark.sql.basic.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午1:05
 * @description: 自定义函数 UDAF
 * 从 Spark3.0 版本后，UserDefinedAggregateFunction 已经不推荐使用了。可以统一采用  Aggregator 强类型聚合函数
 *     	Spark3.0 版本后可以使用 functions.udaf(函数体,函数输入类型) 将 Aggregator 强类型聚合函数 给sql 弱类型使用
 *     	早期版本中，spark不能在sql中使用强类型UDAF操作
 *    	SQL & DSL
 *     	早期的UDAF强类型聚合函数使用DSL语法操作
 */
public class Spark02_SparkSQL_UDAF2 {

	public static void main(String[] args) throws AnalysisException {

		// 创建SparkSQL的运行环境 SparkSession
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		Dataset<Row> rddDF = spark.read().json("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/test.json");

		// 早期版本中，spark不能在sql中使用强类型UDAF操作
		// SQL & DSL
		// 早期的UDAF强类型聚合函数使用DSL语法操作

		//  先将 DataFrame 对象转换成 DataSet
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> rddDS = rddDF.map(new MapFunction<Row, Person>() {
			@Override
			public Person call(Row row) throws Exception {
				return new Person(){{
					Object id = row.getAs("id");
					setId(String.valueOf(id));
					setName(row.getAs("name"));
					Object age = row.getAs("age");
					setAge(String.valueOf(age));
				}};
			}
		}, personEncoder);

		// 将UDAF函数转换为查询的列对象
		TypedColumn<Person, Double> typedColumn = new MyUDAF2().toColumn();

		rddDS.select(typedColumn).show();


		spark.close();


	}

}
