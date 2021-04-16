package com.liu.hadoop.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午2:57
 * @description: RDD 转换算子    mapPartitions
 *
 * 将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处
 * 理，哪怕是过滤数据。
 */
public class Spark02_RDD_Operator_Transform_MapPartitions {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list,2);

		//3. mapPartitions : 可以以分区为单位进行数据转换操作
		//                 但是会将整个分区的数据加载到内存进行引用
		//                 如果处理完的数据是不会被释放掉，存在对象的引用。
		//                 在内存较小，数据量较大的场合下，容易出现内存溢出。
		JavaRDD<Integer> rdd1 = rdd.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
			@Override
			public Iterator<Integer> call(Iterator<Integer> t) throws Exception {
				List<Integer> list = new ArrayList<>();
				while (t.hasNext()) {
					Integer next = t.next();
					list.add(next*2);
				}
				return list.iterator();
			}
		});

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
