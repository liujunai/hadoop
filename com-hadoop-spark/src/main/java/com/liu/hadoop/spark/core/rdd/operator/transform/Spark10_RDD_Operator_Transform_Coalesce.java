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
 * @description:   RDD 转换算子    coalesce
 *
 * 根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
 * 当 spark 程序中，存在过多的小任务的时候，可以通过 coalesce 方法，收缩合并分区，减少
 * 分区的个数，减小任务调度成本
 */
public class Spark10_RDD_Operator_Transform_Coalesce {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list,4);

		//3.coalesce  coalesce方法默认情况下不会将分区的数据打乱重新组合
		//        	这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
		//        	如果想要让数据均衡，可以进行shuffle处理
		JavaRDD<Integer> rdd1 = rdd.coalesce(2,true);

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
