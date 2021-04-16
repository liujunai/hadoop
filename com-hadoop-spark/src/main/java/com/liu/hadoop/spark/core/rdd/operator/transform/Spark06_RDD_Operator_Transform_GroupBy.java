package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
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
public class Spark06_RDD_Operator_Transform_GroupBy {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list,2);

//		List<String> strings = new ArrayList<>();
//		strings.get(0).charAt(0);

		//3.groupBy
		// groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
		// 相同的key值的数据会放置在一个组中
		JavaPairRDD<Integer, Iterable<Integer>> rdd1 = rdd.groupBy(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer integer) throws Exception {

				return integer % 2;
			}
		});


		//4.执行任务
		List<Tuple2<Integer, Iterable<Integer>>> collect = rdd1.collect();

		//5.输出结果
		for (Tuple2<Integer, Iterable<Integer>> integer : collect) {
			System.out.println("integer = " + integer._1  + "  " +integer._2);
		}

		//6.关闭资源
		jsc.close();
	}

}
