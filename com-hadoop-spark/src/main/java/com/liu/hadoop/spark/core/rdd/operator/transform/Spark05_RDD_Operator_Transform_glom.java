package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.*;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子    glom
 *
 * 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
 */
public class Spark05_RDD_Operator_Transform_glom {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list,2);

		//3.glom
		JavaRDD<List<Integer>> rdd1 = rdd.glom();

		// 对每个分区数据处理
		JavaRDD<List<Integer>> map = rdd1.map(new Function<List<Integer>, List<Integer>>() {
			@Override
			public List<Integer> call(List<Integer> v1) throws Exception {
				List<Integer> list = new ArrayList<>();
				list.add(Collections.min(v1));
				list.add(Collections.max(v1));

				return list;
			}
		});


		//4.执行任务
		List<List<Integer>> collect = map.collect();

		//5.输出结果
		for (List<Integer> integer : collect) {
			System.out.println("integer = " + integer);
		}

		//6.关闭资源
		jsc.close();
	}

}
