package com.liu.hadoop.spark.core.rdd.serialization;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/15 下午6:15
 * @description: RDD   序列化    Serializable
 * <p>
 * 从计算的角度, 算子以外的代码都是在 Driver 端执行, 算子里面的代码都是在 Executor 端执行
 *
 * 那么在 scala 的函数式编程中，就会导致算子内经常会用到算子外的数据，这样就
 * 形成了闭包的效果，如果使用的算子外的数据无法序列化，就意味着无法传值给 Executor
 * 端执行，就会发生错误，所以需要在执行任务计算前，检测闭包内的对象是否可以进行序列
 * 化，这个操作我们称之为闭包检测
 */
public class Spark01_RDD_Serializable {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");

		// 替换默认的序列化机制
// 		.set("spark.serializer",
//		"org.apache.spark.serializer.KryoSerializer")
//		 注册需要使用 kryo 序列化的自定义类
//		.registerKryoClasses(Array(classOf[Searcher]))

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建  PairRDD
		List<String> list = Arrays.asList("hello world", "hello spark", "hive", "hadoop");
		JavaRDD<String> rdd = jsc.parallelize(list);

		User user = new User();

		rdd.foreach(new VoidFunction<String>() {
			@Override
			public void call(String s) throws Exception {
				System.out.println("user = " + user.getAge() + 1);
			}
		});


		jsc.stop();

	}

}
