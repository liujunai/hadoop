package com.liu.hadoop.spark.core.broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.*;

/**
 * @author LiuJunFeng
 * @date 2021/4/13 下午7:45
 * @description: RDD  广播变量    分布式共享只读变量
 * <p>
 * 广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个
 * 或多个 Spark 操作使用。比如，如果你的应用需要向所有节点发送一个较大的只读查询表，
 * 广播变量用起来都很顺手。在多个并行操作中使用同一个变量，但是 Spark 会为每个任务
 * 分别发送。
 * <p>
 * 闭包数据，都是以Task为单位发送的，每个任务中包含闭包数据
 * 这样可能会导致，一个Executor中含有大量重复的数据，并且占用大量的内存
 * executor其实就是一个JVM，所有在启动时，会自动分配内存
 * 完全可以将任务中的闭包数据放置在executor的内存中，达到共享的目的
 * spark中的广播变量就可以将闭包的数据保存到executor的内存中
 * spark中的广播变量不能过更改，分布式共享只读变量
 */
public class Spark01_RDD_Broadcast {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<Map<String, Integer>> map1 = Collections.singletonList(
				new HashMap<String, Integer>() {{
					put("a", 1);
					put("b", 2);
					put("c", 3);
					put("d", 4);
				}}
		);
		JavaRDD<Map<String, Integer>> rdd = jsc.parallelize(map1);

		Map<String, Integer> map2 = new HashMap<String, Integer>() {{
			put("a", 5);
			put("b", 6);
			put("c", 7);
		}};

		// 闭包数据，都是以Task为单位发送的，每个任务中包含闭包数据
		// 这样可能会导致，每个Executor中的Task都有一份数据，并且占用大量的内存
		JavaRDD<Map<String, Tuple2<Integer, Integer>>> map = rdd.map(new Function<Map<String, Integer>, Map<String, Tuple2<Integer, Integer>>>() {
			@Override
			public Map<String, Tuple2<Integer, Integer>> call(Map<String, Integer> v1) throws Exception {

				Map<String, Tuple2<Integer, Integer>> results = new HashMap<>();

				for (Map.Entry<String, Integer> entry : v1.entrySet()) {
					if (map2.containsKey(entry.getKey())) {
						results.put(entry.getKey(), new Tuple2<>(entry.getValue(), map2.get(entry.getKey())));
					} else {
						results.put(entry.getKey(), new Tuple2<>(entry.getValue(), null));
					}

				}


				return results;
			}
		});


		//4.获取累加器的值输出结果
		map.collect().forEach(x -> System.out.println("x = " + x));


		//5.关闭资源
		jsc.close();

	}


}
