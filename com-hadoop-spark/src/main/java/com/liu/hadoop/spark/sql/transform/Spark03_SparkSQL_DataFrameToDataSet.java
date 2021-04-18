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
 * DataFrame    -->   DataSet
 */
public class Spark03_SparkSQL_DataFrameToDataSet {

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


		//  DataFrame 对象转换成 DataSet     Dataset<Row> -> Dataset<Person>
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> personDS = personDF.map(new MapFunction<Row, Person>() {
			@Override
			public Person call(Row row) throws Exception {

				return new Person(){{
					setId(row.getAs("id"));
					setName(row.getAs("name"));
					setAge(row.getAs("age"));
				}};
			}
		}, personEncoder);

		personDS.show();
		personDS.printSchema();

	}
}
