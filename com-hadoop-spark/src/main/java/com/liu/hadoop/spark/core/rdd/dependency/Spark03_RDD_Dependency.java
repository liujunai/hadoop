package com.liu.hadoop.spark.core.rdd.dependency;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @author LiuJunFeng
 * @date 2021/4/16 上午1:58
 * @description:    RDD  阶段划分  and  任务划分
 *
 */
public class Spark03_RDD_Dependency {

	public static void main(String[] args) {

		/**
		 * @author LiuJunFeng
		 * @date 2021/4/16 上午2:34
		 * @return void
		 * @Description:  RDD   阶段划分
		 *
		 * DAG（Directed Acyclic Graph）有向无环图是由点和线组成的拓扑图形，该图形具有方向，
		 * 不会闭环。例如，DAG 记录了 RDD 的转换过程和任务的阶段。
		 *
		 */


		/**
		 * @author LiuJunFeng
		 * @date 2021/4/16 上午2:34
		 * @return void
		 * @Description:  RDD  任务划分
		 *
		 * RDD 任务切分中间分为：Application、Job、Stage 和 Task
		 * ⚫ Application：初始化一个 SparkContext 即生成一个 Application；
		 * ⚫ Job：一个 Action 算子就会生成一个 Job；
		 * ⚫ Stage：Stage 等于宽依赖(ShuffleDependency)的个数加 1；
		 * ⚫ Task：一个 Stage 阶段中，最后一个 RDD 的分区个数就是 Task 的个数。
		 * 注意：Application->Job->Stage->Task 每一层都是 1 对 n 的关系
		 *
		 */


	}
}
