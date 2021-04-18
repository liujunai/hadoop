package com.liu.hadoop.spark.core.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/13 下午7:45
 * @description: RDD  统计案例
 */
public class Spark02_RDD_Request {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从文件中创建RDD，将文件中的数据作为处理的数据源
		JavaRDD<String> rdd = jsc.textFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/user_visit_action.txt");

		// Q : actionRDD重复使用
		// Q : cogroup性能可能较低
		rdd.cache();

		//3.统计品类的点击数量：（品类ID，点击数量）
		JavaRDD<String> clickActionRDD = rdd.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] s1 = s.split("_");
				return !("-1").equals(s1[6]);
			}
		});
		JavaPairRDD<String, Integer> clickCountRDD = clickActionRDD.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				String[] s1 = s.split("_");
				return new Tuple2<>(s1[6], 1);
			}
		}).reduceByKey(Integer::sum);

		//4.统计品类的下单数量：（品类ID，下单数量）
		JavaRDD<String> orderActionRDD = rdd.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] s1 = s.split("_");
				return s1[8] != null;
			}
		});
		JavaPairRDD<String, Integer> orderCountRDD = orderActionRDD.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) throws Exception {
				String[] s1 = s.split("_");
				String str = s1[8];
				return Arrays.asList(str.split(",")).iterator();
			}
		}).mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<>(s, 1);
			}
		}).reduceByKey(Integer::sum);

		//5.统计品类的支付数量：（品类ID，支付数量）
		JavaRDD<String> payActionRDD = rdd.filter((Function<String, Boolean>) s -> {
			String[] s1 = s.split("_");
			return s1[10] != null;
		});
		JavaPairRDD<String, Integer> payCountRDD = payActionRDD.flatMap((FlatMapFunction<String, String>) s -> {
			String[] s1 = s.split("_");
			String str = s1[10];
			return Arrays.asList(str.split(",")).iterator();
		}).mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
				.reduceByKey(Integer::sum);

		// 6. 将品类进行排序，并且取前10名
		//    点击数量排序，下单数量排序，支付数量排序
		//    元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
		//    ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )
		JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> rdd1 = clickCountRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Tuple3<Integer, Integer, Integer>>() {
			@Override
			public Tuple2<String, Tuple3<Integer, Integer, Integer>> call(Tuple2<String, Integer> v1) throws Exception {
				return new Tuple2<>(v1._1(), new Tuple3<>(v1._2(), 0, 0));
			}
		});
		JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> rdd2 = orderCountRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Tuple3<Integer, Integer, Integer>>() {
			@Override
			public Tuple2<String, Tuple3<Integer, Integer, Integer>> call(Tuple2<String, Integer> v1) throws Exception {
				return new Tuple2<>(v1._1(), new Tuple3<>(0, v1._2(), 0));
			}
		});
		JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> rdd3 = payCountRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Tuple3<Integer, Integer, Integer>>() {
			@Override
			public Tuple2<String, Tuple3<Integer, Integer, Integer>> call(Tuple2<String, Integer> v1) throws Exception {
				return new Tuple2<>(v1._1(), new Tuple3<>(0, 0, v1._2()));
			}
		});

		// 7. 将三个数据源合并在一起，统一进行聚合计算
		JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> union = rdd1.union(rdd2).union(rdd3);

		JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> soruceRDD = union.reduceByKey(new Function2<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
			@Override
			public Tuple3<Integer, Integer, Integer> call(Tuple3<Integer, Integer, Integer> v1, Tuple3<Integer, Integer, Integer> v2) throws Exception {
				return new Tuple3<>(v1._1() + v2._1(), v1._2() + v2._2(), v1._3() + v2._3());
			}
		});

		// scala.Tuple3 cannot be cast to java.lang.Comparable   元组类型无法排序  暂时停下
		List<Tuple2<String, Tuple3<Integer, Integer, Integer>>> resultRDD = soruceRDD.sortByKey(false).take(10);

		//7.执行任务
		resultRDD.forEach(x -> System.out.println("x = " + x));

		//4.输出结果


		//5.关闭资源
		jsc.close();

	}


}
