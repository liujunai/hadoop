package com.liu.hadoop.flink.transform;

import com.liu.hadoop.flink.beans.Sensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description:  Transform  转换算子  单数据流基本转换  map flatMap Filter
 *
 *
 */
public class Flink01_Transform_Base {

	public static void main(String[] args) {

		//创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		// 从文件读取数据
		DataStream<String> dataStream = env.readTextFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-flink/data/sensor.txt");

		// 1. map，把String转换成长度输出
		DataStream<Integer> mapStream = dataStream.map(new MapFunction<String, Integer>() {
			@Override
			public Integer map(String value) throws Exception {
				return value.length();
			}
		});

		// 2. flatmap，按逗号分字段
		DataStream<String> flatMapStream = dataStream.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
				String[] fields = value.split(",");
				for( String field: fields )
					out.collect(field);
			}
		});

		// 3. filter, 筛选11开头的id对应的数据
		DataStream<String> filterStream = dataStream.filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String value) throws Exception {
				return value.startsWith("11");
			}
		});

		// 打印输出
		mapStream.print("map");
		flatMapStream.print("flatMap");
		filterStream.print("filter");

		//执行
		try {
			env.execute("demoTask1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
