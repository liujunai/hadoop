package com.liu.hadoop.flink.window;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description: window function：窗口函数
 *
 * window function :
 * 		.trigger() —— 触发器   定义 window 什么时候关闭，触发计算并输出结果
 * 		.evitor() —— 移除器   定义移除某些数据的逻辑
 * 		.allowedLateness() —— 允许处理迟到的数据
 *  	.sideOutputLateData() —— 将迟到的数据放入侧输出流
 * 		.getSideOutput() —— 获取侧输出流
 *
 */
public class Flink08_WindowFunction {

	public static void main(String[] args) {

		//创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//从 socket 文本流读取数据
		DataStream<String> dataStream = env.socketTextStream("localhost", 8888);

		DataStream<Tuple2<String,Integer>> map = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
			@Override
			public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
				String[] split = s.split(" ");
				for (String s1 : split) {
					collector.collect(new Tuple2<>(s1,1));
				}
			}
		});

		// 定义侧输出流
		OutputTag<Tuple2<String, Integer>> late = new OutputTag<>("late");

		SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = map.keyBy(0).timeWindow(Time.seconds(15))
				// 触发器
				.trigger(new MyTrigger())
				// 移除器
				.evictor(new MyEvictor())
				// 允许处理迟到的数据 等待时间
				.allowedLateness(Time.minutes(1))
				// 获取侧输出流 将迟到的数据放入侧输出流
				.sideOutputLateData(late)
				// 聚合函数
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
						return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
					}
				});

		// 获取侧输出流
		DataStream<Tuple2<String, Integer>> output = reduce.getSideOutput(late);


		//执行
		try {
			env.execute("demoTask1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
