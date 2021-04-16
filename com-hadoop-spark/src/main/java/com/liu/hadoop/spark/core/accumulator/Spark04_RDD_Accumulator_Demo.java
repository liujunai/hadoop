package com.liu.hadoop.spark.core.accumulator;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;


/**
 * @author LiuJunFeng
 * @date 2021/4/16 下午3:12
 * @description:
 */
public class Spark04_RDD_Accumulator_Demo {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

		SparkContext sc = spark.sparkContext();

		// 内置的累加器有三种，LongAccumulator、DoubleAccumulator、CollectionAccumulator

		// LongAccumulator: 数值型累加

		LongAccumulator longAccumulator = sc.longAccumulator("long-account");

		// DoubleAccumulator: 小数型累加

		DoubleAccumulator doubleAccumulator = sc.doubleAccumulator("double-account");

		// CollectionAccumulator：集合累加

		CollectionAccumulator collectionAccumulator = sc.collectionAccumulator("double-account");

		Dataset num1 = spark.createDataset(Arrays.asList(1, 2, 3), Encoders.INT());

		Dataset num2 = num1.map((MapFunction) x -> {
			longAccumulator.add((Long) x);

			doubleAccumulator.add((Double) x);

			collectionAccumulator.add(x);

			return x;

		}, Encoders.INT()).cache();

		num2.count();

		System.out.println("longAccumulator: " + longAccumulator.value());

		System.out.println("doubleAccumulator: " + doubleAccumulator.value());

		// 注意，集合中元素的顺序是无法保证的，多运行几次发现每次元素的顺序都可能会变化

		System.out.println("collectionAccumulator: " + collectionAccumulator.value());

	}

}
