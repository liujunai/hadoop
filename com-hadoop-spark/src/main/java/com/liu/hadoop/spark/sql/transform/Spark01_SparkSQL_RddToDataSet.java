package com.liu.hadoop.spark.sql.transform;

import com.liu.hadoop.spark.sql.basic.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午3:44
 * @description: RDD   DataFrame  DataSet  相互转换
 * <p>
 * RDD  --> Dataset
 */
public class Spark01_SparkSQL_RddToDataSet {

	public static void main(String[] args) throws AnalysisException {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// 定义 JavaRDD<Person>
		// 定义RDD
		JavaRDD<Row> rdd = spark.read().json("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/test.json").javaRDD();
		// map()函数把RDD转换成Person对象
		JavaRDD<Person> map = rdd.map(new Function<Row, Person>() {
			@Override
			public Person call(Row row) throws Exception {
				Person person = new Person();
				person.setId(String.valueOf(row.get(1)));
				person.setName(row.getString(2));
				person.setAge(String.valueOf(row.get(0)));
				return person;
			}
		});

		//  RDD 对象转换成 Dataset   JavaRDD<Person> -> Dataset<Person>
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> dataSet = spark.createDataset(map.rdd(), personEncoder);


		// spark sql 方式调用
//		dataSet.createTempView("user");
//		dataSet.sqlContext().sql("select * from user").show();

		// spark core 方式调用   注释： Function 方法换成 MapFunction
		Encoder<String> encoder = Encoders.STRING();
		Dataset<String> fieldValue = dataSet.map(new MapFunction<Person, String>() {
			@Override
			public String call(Person person) throws Exception {
				return person.getName();
			}
		}, encoder);
		fieldValue.show();


	}
}
