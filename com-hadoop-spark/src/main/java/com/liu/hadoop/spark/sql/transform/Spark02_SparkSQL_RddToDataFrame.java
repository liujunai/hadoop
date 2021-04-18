package com.liu.hadoop.spark.sql.transform;

import com.liu.hadoop.spark.sql.basic.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Function1;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午3:44
 * @description: RDD   DataFrame  DataSet  相互转换
 * <p>
 * RDD --> DataFrame
 */
public class Spark02_SparkSQL_RddToDataFrame {

	public static void main(String[] args) throws AnalysisException {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

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

		//  RDD  对象转换成 DataFrame  JavaRDD<Person>  -> Dataset<Row>
		Dataset<Row> dataFrame = spark.createDataFrame(map, Person.class);


		// spark sql 方式调用
//		dataFrame.createTempView("user");
//		spark.sql("select * from user").show();

		// spark core 方式调用   注释： Function 方法换成 MapFunction
		Encoder<String> encoder = Encoders.STRING();
		Dataset<String> fieldValue = dataFrame.map(new MapFunction<Row, String>() {
			@Override
			public String call(Row value) {
				return value.getAs("name");
			}
		}, encoder);
		fieldValue.show();


	}
}
