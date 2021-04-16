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
 * @description:   RDD 转换算子     sample
 *
 * 根据指定的规则从数据集中抽取数据
 */
public class Spark08_RDD_Operator_Transform_Sample {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd = jsc.parallelize(list);

		//3.sample
		// 抽取数据不放回（伯努利算法）
		// 伯努利算法：又叫 0、1 分布。例如扔硬币，要么正面，要么反面。
		// 具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
		// 第一个参数：抽取的数据是否放回，false：不放回
		// 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
		// 第三个参数：随机数种子

		JavaRDD<Integer> rdd1 = rdd.sample(false,0.5);

		// 抽取数据放回（泊松算法）
		// 第一个参数：抽取的数据是否放回，true：放回；false：不放回
		// 第二个参数：重复数据的几率，范围大于等于 0.表示每一个元素被期望抽取到的次数
		// 第三个参数：随机数种子
		JavaRDD<Integer> rdd2 = rdd.sample(true,2);


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
