package com.liu.hadoop.spark.core.accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author LiuJunFeng
 * @date 2021/4/13 下午7:45
 * @description: RDD  累加器   自定义累加器
 */
public class Spark03_RDD_Accumulator {

	public static void main(String[] args) {

		//1.准备环境
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("demo");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		//2.从内存中创建RDD
		List<String> list = Arrays.asList("hello hadoop", "hello spark", "hello hive", "hello hadoop", "hello spark");
		JavaRDD<String> rdd = jsc.parallelize(list);

		// 创建累加器对象
		MyAccumulator myAccumulator = new MyAccumulator();
		// 向Spark进行注册
		jsc.sc().register(myAccumulator);

		// 数据的累加（使用累加器）
		rdd.foreach(x -> myAccumulator.add(x));

		// 获取累加器累加的结果
		System.out.println("myAccumulator = " + myAccumulator.value());

		//5.关闭资源
		jsc.close();

	}


}

/**
 * @author LiuJunFeng
 * @date 2021/4/16 下午3:47
 * @return
 * @Description: 自定义数据累加器：WordCount
 * 1. 继承AccumulatorV2, 定义泛型
 * IN : 累加器输入的数据类型 String
 * OUT : 累加器返回的数据类型  Map[String, Integer]
 * <p>
 * 2. 重写方法（6）
 */
class MyAccumulator extends AccumulatorV2<String, Map<String, Integer>> {

	Map<String, Integer> map = new HashMap<>();

	// 判断是否初始状态
	@Override
	public boolean isZero() {
		return map.isEmpty();
	}

	// 复制累加器
	@Override
	public AccumulatorV2<String, Map<String, Integer>> copy() {
		return new MyAccumulator();
	}

	// 清空累加器
	@Override
	public void reset() {
		map.clear();
	}

	// 获取累加器需要计算的值
	@Override
	public void add(String v) {

		String[] s = v.split(" ");
		for (String s1 : s) {
			if (map.containsKey(s1)) {
				Integer integer = map.get(s1);
				map.put(s1, integer + 1);
			} else {
				map.put(s1, 1);
			}
		}
	}

	// Driver合并多个累加器
	@Override
	public void merge(AccumulatorV2<String, Map<String, Integer>> v2) {

		Map<String, Integer> map2 = v2.value();

		for (Map.Entry<String, Integer> entry : map2.entrySet()) {

			if (map.containsKey(entry.getKey())) {
				Integer integer = map.get(entry.getKey());
				map.put(entry.getKey(), integer + entry.getValue());
			} else {
				map.put(entry.getKey(), entry.getValue());
			}
		}
	}

	// 累加器结果
	@Override
	public Map<String, Integer> value() {
		return map;
	}
}