package com.liu.hadoop.spark.sql.transform;

import com.liu.hadoop.spark.sql.basic.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午3:44
 * @description: RDD   DataFrame  DataSet  相互转换
 * <p>
 * DataSet    -->   DataFrame
 */
public class Spark03_SparkSQL_DataSetToDataFrame {

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

		// RowEncoder 转换格式
		List<StructField> fieldList = new ArrayList<>();
		fieldList.add(DataTypes.createStructField("id", DataTypes.StringType, false));
		fieldList.add(DataTypes.createStructField("name", DataTypes.StringType, false));
		fieldList.add(DataTypes.createStructField("age", DataTypes.StringType, false));
		StructType rowSchema = DataTypes.createStructType(fieldList);
		ExpressionEncoder<Row> rowEncoder = RowEncoder.apply(rowSchema);

		// Dataset 对象转换成 DataFrame     Dataset<Person> -> Dataset<Row>
		Dataset<Row> personDF = personDS.map(new MapFunction<Person, Row>() {
			@Override
			public Row call(Person person) throws Exception {
				List<Object> objectList = new ArrayList<>();
				objectList.add(person.getId());
				objectList.add(person.getName());
				objectList.add(person.getAge());
				return RowFactory.create(objectList.toArray());
			}
		},rowEncoder);
		personDF.show();
		personDF.printSchema();


	}
}
