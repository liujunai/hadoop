package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子    combineByKey
 * <p>
 * 最通用的对 key-value 型 rdd 进行聚集操作的聚集函数（aggregation function）。类似于
 * aggregate()，combineByKey()允许用户返回值的类型与输入不一致。
 *
 * ------------------------------------------------------------------------------
 * reduceByKey: 相同 key 的第一个数据不进行任何计算，分区内和分区间计算规则相同
 *
 * FoldByKey: 相同 key 的第一个数据和初始值进行分区内计算，分区内和分区间计算规则相同
 *
 * AggregateByKey：相同 key 的第一个数据和初始值进行分区内计算，分区内和分区间计算规则可以不相同
 *
 * CombineByKey:当计算时，发现数据结构不满足要求时，可以让第一个数据转换结构。分区内和分区间计算规则不相同。
 *
 *
 */
public class Spark19_RDD_Operator_Transform_CombineByKey {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建  PairRDD
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 60),
				new Tuple2<String, Integer>("class1", 60),
				new Tuple2<String, Integer>("class1", 60),
				new Tuple2<String, Integer>("class2", 60),
				new Tuple2<String, Integer>("class2", 50)
		);
		//平行化集合 生成JavaPairRDD  此处使用的是parallelizePairs
		JavaPairRDD<String, Integer> rdd = jsc.parallelizePairs(scoreList, 2);

		//3.combineByKey :方法需要三个参数
		//           第一个参数表示：将相同key的第一个数据进行结构的转换，实现操作
		//           第二个参数表示：分区内的计算规则
		//           第三个参数表示：分区间的计算规则
		JavaPairRDD<String, Tuple2<Integer, Integer>> rdd11 = rdd.combineByKey(
				x -> new Tuple2<Integer, Integer>(x, 1),
				(v1, x) -> new Tuple2<>(v1._1() + x, v1._2() + 1),
				(v1, v2) -> new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2()));


//		JavaPairRDD<String, Tuple2<Integer, Integer>> rdd1 = rdd.combineByKey(new Function<Integer, Tuple2<Integer, Integer>>() {
//			@Override
//			public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
//				return new Tuple2<>(integer, 1);
//			}
//		}, new Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>() {
//			@Override
//			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Integer integer) throws Exception {
//				return new Tuple2<>(v1._1() + integer, v1._2() + 1);
//			}
//		}, new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
//			@Override
//			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
//				return new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2());
//			}
//		});

		JavaPairRDD<String, Integer> rdd2 = rdd11.mapValues(v1 -> v1._1() / v1._2());

		//4.执行任务
		List<Tuple2<String, Integer>> collect = rdd2.collect();

		//5.输出结果
		collect.forEach(x -> System.out.println("key = " + x._1() + "  value = " + x._2()));

		//6.关闭资源
		jsc.close();
	}

}
