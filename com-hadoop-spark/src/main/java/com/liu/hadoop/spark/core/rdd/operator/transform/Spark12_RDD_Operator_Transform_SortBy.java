package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description:   RDD 转换算子    sortBy
 *
 * 该操作用于排序数据。在排序之前，可以将数据通过 f 函数进行处理，之后按照 f 函数处理
 * 的结果进行排序，默认为升序排列。排序后新产生的 RDD 的分区数与原 RDD 的分区数一
 * 致。中间存在 shuffle 的过程
 */
public class Spark12_RDD_Operator_Transform_SortBy {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list,4);

		//3.sortBy
		// 第一个参数是一个函数，该函数的也有一个带T泛型的参数，返回类型和RDD中元素的类型是一致的；
		// 第二个参数是ascending，从字面的意思大家应该可以猜到，是的，这参数决定排序后RDD中的元素是升序还是降序，默认是true，也就是升序；
		// 第三个参数是numPartitions，该参数决定排序后的RDD的分区个数，默认排序后的分区个数和排序之前的个数相等，即为this.partitions.size。
		// sortBy默认情况下，不会改变分区。但是中间存在shuffle操作
		// Java版本 必须是三个参数！

		JavaRDD<Integer> rdd1 = rdd.sortBy(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer integer) throws Exception {
				return integer;
			}
		}, false, 2);

		//4.执行任务
		List<Integer> collect = rdd1.collect();

		//5.输出结果
		for (Integer integer : collect) {
			System.out.println("integer = " + integer);
		}

		//6.关闭资源
		jsc.close();
	}

}
