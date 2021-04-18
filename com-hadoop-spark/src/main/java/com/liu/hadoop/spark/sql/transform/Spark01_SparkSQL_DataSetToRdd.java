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
 * Dataset  -->  RDD
 */
public class Spark01_SparkSQL_DataSetToRdd {

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

		// 定义 Dataset    Dataset<Person>
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> personDS = spark.createDataset(personList, personEncoder);
//		personDS.show();
//		personDS.printSchema();


		//  Dataset 对象转换成 RDD
		// Dataset<Person> -> JavaRDD<Person>
		JavaRDD<Person> personJavaRDD = personDS.toJavaRDD();

		// Dataset<Person> -> JavaRDD<Row>
		JavaRDD<Row> rowJavaRDD = personDS.toJavaRDD().map(new Function<Person, Row>() {
			@Override
			public Row call(Person person) throws Exception {
				return null;
			}
		});

		personJavaRDD.foreach(x -> System.out.println(x.toString()));

	}
}
