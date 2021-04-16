package com.liu.hadoop.spark.core.rdd.operator.transform;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子   groupBy
 *
 * 将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样
 * 的操作称之为 shuffle。极限情况下，数据可能被分在同一个分区中
 * 一个组的数据在一个分区中，但是并不是说一个分区中只有一个组
 */
public class Spark06_RDD_Operator_Transform_GroupBy_Test {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		JavaRDD<String> rdd = jsc.textFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/apache.log");

		JavaRDD<Tuple2<String, Integer>> map = rdd.map(new Function<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {

				String[] strings = s.split(" ");
				String str = strings[3];
				SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
				Date date = dateFormat.parse(str);

				Calendar cal = Calendar.getInstance();
				cal.setTime(date);//指定日期
				String dateHh = String.valueOf(cal.get(Calendar.HOUR_OF_DAY));

				return new Tuple2<>(dateHh, 1);

			}
		});

		JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupBy = map.groupBy(new Function<Tuple2<String, Integer>, String>() {
			@Override
			public String call(Tuple2<String, Integer> v1) throws Exception {

				return v1._1;
			}
		});

		JavaRDD<Tuple2<String, Integer>> map1 = groupBy.map(new Function<Tuple2<String, Iterable<Tuple2<String, Integer>>>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> v1) throws Exception {

				return new Tuple2<>(v1._1, Iterables.size(v1._2));
			}
		});


		//4.执行任务
		List<Tuple2<String, Integer>> collect = map1.collect();

		//5.输出结果
		for (Tuple2<String, Integer> integer : collect) {
			System.out.println("integer = " + integer._1  + "  " +integer._2);
		}

		//6.关闭资源
		jsc.close();
	}



}
