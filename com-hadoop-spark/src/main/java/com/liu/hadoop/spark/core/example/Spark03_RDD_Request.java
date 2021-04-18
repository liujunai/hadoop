package com.liu.hadoop.spark.core.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * @author LiuJunFeng
 * @date 2021/4/13 下午7:45
 * @description: RDD  统计案例
 */
public class Spark03_RDD_Request {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从文件中创建RDD，将文件中的数据作为处理的数据源
		JavaRDD<String> rdd = jsc.textFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/input/user_visit_action.txt");

		// Q : 存在大量的shuffle操作（reduceByKey）
		// reduceByKey 聚合算子，spark会提供优化，缓存

		// 3. 将数据转换结构
		// Arrays.stream().map Java算子中
		JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> rdd1 = rdd.flatMap(new FlatMapFunction<String, Tuple2<String, Tuple3<Integer, Integer, Integer>>>() {
			@Override
			public Iterator<Tuple2<String, Tuple3<Integer, Integer, Integer>>> call(String s) throws Exception {
				String[] s1 = s.split("_");
				if (!"-1".equals(s1[6])) {
					return Collections.singletonList(new Tuple2<>(s1[6], new Tuple3<>(1, 0, 0))).iterator();
				} else if (s1[8] != null) {
					String[] split1 = s1[8].split(",");

					Stream<Tuple2<String, Tuple3<Integer, Integer, Integer>>> tuple2Stream = Arrays.stream(split1).map(new Function<String, Tuple2<String, Tuple3<Integer, Integer, Integer>>>() {
						@Override
						public Tuple2<String, Tuple3<Integer, Integer, Integer>> apply(String s) {
							return new Tuple2<>(s, new Tuple3<>(0, 1, 0));
						}
					});
					return tuple2Stream.iterator();

				} else if (s1[10] != null) {
					String[] split2 = s1[10].split(",");

					Stream<Tuple2<String, Tuple3<Integer, Integer, Integer>>> tuple2Stream = Arrays.stream(split2).map(new Function<String, Tuple2<String, Tuple3<Integer, Integer, Integer>>>() {
						@Override
						public Tuple2<String, Tuple3<Integer, Integer, Integer>> apply(String s) {
							return new Tuple2<>(s, new Tuple3<>(0, 0, 1));
						}
					});
					return tuple2Stream.iterator();

				} else {
					return null;
				}
			}
		}).mapToPair(new PairFunction<Tuple2<String, Tuple3<Integer, Integer, Integer>>, String, Tuple3<Integer, Integer, Integer>>() {
			@Override
			public Tuple2<String, Tuple3<Integer, Integer, Integer>> call(Tuple2<String, Tuple3<Integer, Integer, Integer>> v1) throws Exception {
				return new Tuple2<>(v1._1(), v1._2());
			}
		});


		// 4. 将相同的品类ID的数据进行分组聚合
		//    ( 品类ID，( 点击数量, 下单数量, 支付数量 ) )
		JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> rdd2 = rdd1.reduceByKey(new Function2<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
			@Override
			public Tuple3<Integer, Integer, Integer> call(Tuple3<Integer, Integer, Integer> v1, Tuple3<Integer, Integer, Integer> v2) throws Exception {
				return new Tuple3<>(v1._1() + v2._1(), v1._2() + v2._2(), v1._3() + v2._3());
			}
		});


		// scala.Tuple3 cannot be cast to java.lang.Comparable   元组类型无法排序  暂时停下
		List<Tuple2<String, Tuple3<Integer, Integer, Integer>>> resultRDD = rdd2.sortByKey(false).take(10);

		//7.执行任务
		resultRDD.forEach(x -> System.out.println("x = " + x));

		//4.输出结果


		//5.关闭资源
		jsc.close();

	}


}
