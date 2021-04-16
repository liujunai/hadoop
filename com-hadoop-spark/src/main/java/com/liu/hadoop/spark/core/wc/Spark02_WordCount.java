package com.liu.hadoop.spark.core.wc;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author LiuJunFeng
 * @date 2021/4/15 下午8:57
 * @description: 示例
 */
public class Spark02_WordCount {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
		JavaSparkContext jsc = new JavaSparkContext(conf);

//		wordCount_groupBy(jsc);
//		wordCount_groupByKey(jsc);
//		wordCount_reduceByKey(jsc);
//		wordCount_aggregateByKey(jsc);
//		wordCount_foldByKey(jsc);
//		wordCount_combineByKey(jsc);
		wordCount_countByKey(jsc);
		wordCount_countByValue(jsc);


		jsc.close();

	}

	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/15 下午9:19
	 * @Description: 1. groupBy   wordCount  案例
	 */
	public static void wordCount_groupBy(JavaSparkContext jsc) {
		List<String> list = Arrays.asList("hello hadoop", "hello spark");
		JavaRDD<String> rdd = jsc.parallelize(list);

		JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) throws Exception {
				return Arrays.asList(s.split(" ")).iterator();
			}
		});

		JavaPairRDD<String, Iterable<String>> groupBy = words.groupBy(new Function<String, String>() {
			@Override
			public String call(String s) throws Exception {
				return s;
			}
		});

		JavaPairRDD<String, Integer> wordCount = groupBy.mapValues(new Function<Iterable<String>, Integer>() {
			@Override
			public Integer call(Iterable<String> v1) throws Exception {
				return Iterables.size(v1);
			}
		});

		wordCount.collect().forEach(x -> System.out.println("x = " + x));
	}

	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/15 下午9:19
	 * @Description: 2. groupByKey   wordCount  案例
	 */
	public static void wordCount_groupByKey(JavaSparkContext jsc) {
		List<String> list = Arrays.asList("hello hadoop", "hello spark");
		JavaRDD<String> rdd = jsc.parallelize(list);

		JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) throws Exception {
				return Arrays.asList(s.split(" ")).iterator();
			}
		});

		JavaPairRDD<String, Integer> wordOne = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<>(s, 1);
			}
		});

		JavaPairRDD<String, Iterable<Integer>> groupByKey = wordOne.groupByKey();

		JavaPairRDD<String, Integer> workCont = groupByKey.mapValues(new Function<Iterable<Integer>, Integer>() {
			@Override
			public Integer call(Iterable<Integer> v1) throws Exception {
				return Iterables.size(v1);
			}
		});

		workCont.collect().forEach(x -> System.out.println("x = " + x));
	}


	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/15 下午9:19
	 * @Description: 3. reduceByKey   wordCount  案例
	 */
	public static void wordCount_reduceByKey(JavaSparkContext jsc) {
		List<String> list = Arrays.asList("hello hadoop", "hello spark");
		JavaRDD<String> rdd = jsc.parallelize(list);

		JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) throws Exception {
				return Arrays.asList(s.split(" ")).iterator();
			}
		});

		JavaPairRDD<String, Integer> wordOne = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> wordCount = wordOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		wordCount.collect().forEach(x -> System.out.println("x = " + x));
	}


	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/15 下午9:19
	 * @Description: 4. aggregateByKey   wordCount  案例
	 */
	public static void wordCount_aggregateByKey(JavaSparkContext jsc) {
		List<String> list = Arrays.asList("hello hadoop", "hello spark");
		JavaRDD<String> rdd = jsc.parallelize(list);

		JavaRDD<String> words = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());

		JavaPairRDD<String, Integer> wordOne = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

		JavaPairRDD<String, Integer> wordCount = wordOne.aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}, new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		wordCount.collect().forEach(x -> System.out.println("x = " + x));
	}

	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/15 下午9:19
	 * @Description: 5. foldByKey   wordCount  案例
	 */
	public static void wordCount_foldByKey(JavaSparkContext jsc) {
		List<String> list = Arrays.asList("hello hadoop", "hello spark");
		JavaRDD<String> rdd = jsc.parallelize(list);

		JavaRDD<String> words = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());

		JavaPairRDD<String, Integer> wordOne = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

		JavaPairRDD<String, Integer> wordCount = wordOne.foldByKey(0, new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		wordCount.collect().forEach(x -> System.out.println("x = " + x));

	}

	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/15 下午9:19
	 * @Description: 6. combineByKey   wordCount  案例
	 */
	public static void wordCount_combineByKey(JavaSparkContext jsc) {
		List<String> list = Arrays.asList("hello hadoop", "hello spark");
		JavaRDD<String> rdd = jsc.parallelize(list);

		JavaRDD<String> words = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());

		JavaPairRDD<String, Integer> wordOne = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

		JavaPairRDD<String, Integer> wordCount = wordOne.combineByKey(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer v1) throws Exception {
				return v1;
			}
		}, new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}, new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		wordCount.collect().forEach(x -> System.out.println("x = " + x));
	}

	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/15 下午9:19
	 * @Description: 7. countByKey   wordCount  案例
	 */
	public static void wordCount_countByKey(JavaSparkContext jsc) {
		List<String> list = Arrays.asList("hello hadoop", "hello spark");
		JavaRDD<String> rdd = jsc.parallelize(list);

		JavaRDD<String> words = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());

		JavaPairRDD<String, Integer> wordOne = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

		Map<String, Long> wordCount = wordOne.countByKey();

		System.out.println(wordCount);
	}

	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/15 下午9:19
	 * @Description: 8. countByValue   wordCount  案例
	 */
	public static void wordCount_countByValue(JavaSparkContext jsc) {
		List<String> list = Arrays.asList("hello hadoop", "hello spark");
		JavaRDD<String> rdd = jsc.parallelize(list);

		JavaRDD<String> words = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());

		Map<String, Long> wordCount = words.countByValue();

		System.out.println(wordCount);
	}

	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/15 下午9:19
	 * @Description: 9. reduce   wordCount  案例
	 */
	public static void wordCount_reduce(JavaSparkContext jsc) {

	}

	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/15 下午9:19
	 * @Description: 10. aggregate   wordCount  案例
	 */
	public static void wordCount_aggregate(JavaSparkContext jsc) {

	}

	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/15 下午9:19
	 * @Description: 11. fold   wordCount  案例
	 */
	public static void wordCount_fold(JavaSparkContext jsc) {

	}


}
