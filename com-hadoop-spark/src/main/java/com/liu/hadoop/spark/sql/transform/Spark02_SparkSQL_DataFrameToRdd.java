package com.liu.hadoop.spark.sql.transform;

import com.liu.hadoop.spark.sql.basic.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午3:44
 * @description: RDD   DataFrame  DataSet  相互转换
 * <p>
 * DataFrame -->   RDD
 */
public class Spark02_SparkSQL_DataFrameToRdd {

	public static void main(String[] args) throws AnalysisException {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		List<Person> personList = Arrays.asList(
				new Person() {{
					setId("1");
					setName("aa");
					setAge("25");
				}},
				new Person() {{
					setId("2");
					setName("bb");
					setAge("30");
				}},
				new Person() {{
					setId("3");
					setName("cc");
					setAge("35");
				}}
		);

		// 定义 DataFrame   DataFrame<Row>
		Dataset<Row> personDF = spark.createDataFrame(personList, Person.class);
//		personDF.show();
//		personDF.printSchema();


		//   DataFrame 对象转换成 RDD
		// Dataset<Row>  ->  JavaRDD<Person>
		JavaRDD<Person> personJavaRDD = personDF.toJavaRDD().map(new Function<Row, Person>() {
			@Override
			public Person call(Row row) throws Exception {
				String id = row.getAs("id");
				String name = row.getAs("name");
				String age = row.getAs("age");
				return new Person() {{
					setId(id);
					setName(name);
					setAge(age);
				}};
			}
		});

		// Dataset<Row>  ->  JavaRDD<Row>
		JavaRDD<Row> rowJavaRDD = personDF.toJavaRDD();

		personJavaRDD.foreach(x -> System.out.println(x.toString()));


	}
}
