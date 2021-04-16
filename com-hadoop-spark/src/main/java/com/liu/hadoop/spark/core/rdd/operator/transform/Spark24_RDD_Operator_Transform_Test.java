package com.liu.hadoop.spark.core.rdd.operator.transform;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.liu.hadoop.spark.util.SortMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子   练习
 */
public class Spark24_RDD_Operator_Transform_Test {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.获取原始数据：
		JavaRDD<String> rdd = jsc.textFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/agent.log");

		//3. 将原始数据进行结构的转换。方便统计
		JavaPairRDD<Tuple2<String, String>, Integer> mapToPair = rdd.map(new Function<String, Tuple2<Tuple2<String, String>, Integer>>() {
			@Override
			public Tuple2<Tuple2<String, String>, Integer> call(String s) throws Exception {

				String str[] = s.split(" ");

				return new Tuple2<>(new Tuple2<>(str[1], str[4]), 1);
			}
		}).mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, Tuple2<String, String>, Integer>() {
			@Override
			public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<Tuple2<String, String>, Integer> v1) throws Exception {
				return new Tuple2<>(v1._1(), v1._2());
			}
		});

		//4. 将转换结构后的数据，进行分组聚合
		JavaPairRDD<Tuple2<String, String>, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer integer, Integer integer2) throws Exception {
				return integer + integer2;
			}
		});

		//5. 将聚合的结果进行结构的转换
		JavaPairRDD<String, Tuple2<String, Integer>> mapToPair1 = reduceByKey.map(new Function<Tuple2<Tuple2<String, String>, Integer>, Tuple2<String, Tuple2<String, Integer>>>() {
			@Override
			public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> v1) throws Exception {
				return new Tuple2<>(v1._1()._1(), new Tuple2<>(v1._1()._2(), v1._2()));
			}
		}).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Integer>>, String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<String, Tuple2<String, Integer>> v1) throws Exception {
				return new Tuple2<>(v1._1(), v1._2());
			}
		});

		JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupByKey = mapToPair1.groupByKey();

		JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> mapValues = groupByKey.mapValues(new Function<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>() {
			@Override
			public Iterable<Tuple2<String, Integer>> call(Iterable<Tuple2<String, Integer>> v1) throws Exception {

				List<Tuple2<String, Integer>> list = new ArrayList<>();
				Map<String, Integer> map = new HashMap<>();

				for (Tuple2<String, Integer> tuple2 : v1) {
					map.put(tuple2._1(),tuple2._2());
				}
				SortMap sortMap = new SortMap();
				Map<String, Integer> map1 = sortMap.sortMap(map);

				for (String s : map1.keySet()) {
					list.add(new Tuple2<>(s,map1.get(s)));
				}
				return list;
			}
		});


		//4.执行任务
		List<Tuple2<String, Iterable<Tuple2<String, Integer>>>> collect = mapValues.collect();

		//5.输出结果
		for (Tuple2<String, Iterable<Tuple2<String, Integer>>> integer : collect) {
			System.out.println("integer = " + integer._1 + "  " + integer._2);
		}


		//6.关闭资源
		jsc.close();
	}


}
