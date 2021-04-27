package com.liu.hadoop.flink.transform;

import com.liu.hadoop.flink.beans.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description:  Transform   Rich Functions   富函数
 *
 * “富函数”是 DataStream API 提供的一个函数类的接口，所有 Flink 函数类都有其 Rich 版本。它与常规函数的不同在于，可以获取运行环境的上下文，
 * 并拥有一些生命周期方法，所以可以实现更复杂的功能。
 *
 * Rich Function 有一个生命周期的概念。典型的生命周期方法有：
 *  	open()方法是 rich function 的初始化方法，当一个算子例如 map 或者 filter被调用之前 open()会被调用。
 *  	close()方法是生命周期中的最后一个调用的方法，做一些清理工作。
 *  	getRuntimeContext()方法提供了函数的 RuntimeContext 的一些信息，例如函数执行的并行度，任务的名字，以及 state 状态
 */
public class Flink04_Transform_RichFunction {

	public static void main(String[] args) {

		//创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		// 从文件读取数据
		DataStream<String> dataStream = env.readTextFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-flink/data/sensor.txt");

		// 数据转换
		DataStream<Sensor> sensorMap = dataStream.map(new MyRichMapFunction());

		sensorMap.print();

		//执行
		try {
			env.execute("demoTask1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 实现自定义富函数类
	public static class MyRichMapFunction extends RichMapFunction<String, Sensor> {

		@Override
		public Sensor map(String s) throws Exception {
			String[] s1 = s.split(",");
			return new Sensor(s1[0], new Long(s1[1]), new Double(s1[2]));
		}
		@Override
		public void open(Configuration parameters) throws Exception {
			// 初始化工作，一般是定义状态，或者建立数据库连接
			System.out.println("open");
		}
		@Override
		public void close() throws Exception {
			// 一般是关闭连接和清空状态的收尾操作
			System.out.println("close");
		}
	}

}
