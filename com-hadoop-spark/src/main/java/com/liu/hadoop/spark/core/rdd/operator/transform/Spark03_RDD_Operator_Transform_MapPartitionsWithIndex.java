package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子     mapPartitionsWithIndex
 * <p>
 * 将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处
 * 理，哪怕是过滤数据，在处理时同时可以获取当前分区索引。
 */
public class Spark03_RDD_Operator_Transform_MapPartitionsWithIndex {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list, 2);

		//3.mapPartitionsWithIndex 将分区号一并传入
		JavaRDD<Tuple2<Integer, Integer>> rdd1 = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Tuple2<Integer, Integer>>>() {
			@Override
			public Iterator<Tuple2<Integer, Integer>> call(Integer v1, Iterator<Integer> v2) throws Exception {

				List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
				while (v2.hasNext()) {
					Integer next = v2.next();

					list.add(new Tuple2<>(v1, next));

				}

				return list.iterator();
			}
		}, false);

		//4.执行任务
		List<Tuple2<Integer, Integer>> collect = rdd1.collect();

		//5.输出结果
		for (Tuple2 integer : collect) {
			System.out.println("分区编号 = " + integer._1 + " 分区数据=" + integer._2);
		}

		//6.关闭资源
		jsc.close();
	}

}
