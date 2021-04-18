package com.liu.hadoop.spark.sql.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午1:07
 * @description: DataSet 是分布式数据集合。DataSet 是 Spark 1.6 中添加的一个新抽象，是 DataFrame 的一个扩展
 * <p>
 * DataSet 是具有强类型的数据集合，需要提供对应的(样列类)类型信息
 * <p>
 * spark提供了一种全新的RDD叫做DataFrame,但是SparkSQL返回值一直是DataSet
 * DataFrame在Scala,Java,Python和R中都支持,在Java中为Dataset
 * Java中 DataFrame 和 Dataset  没有名称上的区别
 */
public class Spark02_SparkSQL_DataSet {

	public static void main(String[] args) {

		// 创建SparkSQL的运行环境 SparkSession
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkSql");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		List<Person> people = Arrays.asList(
				new Person() {{
					setId("1");
					setName("aa");
					setAge("20");
				}}, new Person() {{
					setId("2");
					setName("bb");
					setAge("25");
				}}, new Person() {{
					setId("3");
					setName("cc");
					setAge("30");
				}}
		);

		// 编码器
		Encoder<Person> encoder = Encoders.bean(Person.class);

		// DataFrame 其实是特定泛型的DataSet
		// DataFrame 在RDD的基础上增加了Schema(描述数据的信息,可以认为是元数据,DataFrame曾经就叫做schemaRDD)
		// DataSet   做了强类型支持,在RDD的每一行都做了数据类型约束
		Dataset<Person> dataset = spark.createDataset(people, encoder);

		dataset.select(col("id"),col("name").as("NAME"),col("age")).show();



		spark.close();


	}

}
