package com.liu.hadoop.flink.source;

import com.liu.hadoop.flink.beans.Sensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description: 从集合中读取数据
 */
public class Flink01_Source_Collection {

	public static void main(String[] args) {

		//创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//全局并行度
		env.setParallelism(1);

		//从集合中读取数据
		DataStream<Sensor> dataStream = env.fromCollection(Arrays.asList(
				new Sensor("11", 1546874524L, 36.5),
				new Sensor("22", 1546874524L, 36.5),
				new Sensor("33", 1546874524L, 36.5),
				new Sensor("44", 1546874524L, 36.5)));

		//从指定元素中读取数据
		DataStream<String> dataStreamString = env.fromElements("1", "3", "4", "7", "86");

		//打印输出
		dataStream.print("集合输出");
		dataStreamString.print("元素输出");

		//执行
		try {
			env.execute("demoTask1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
