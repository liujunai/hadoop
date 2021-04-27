package com.liu.hadoop.flink.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description: window function：窗口函数  -->增量聚合函数
 *
 * window function 定义了要对窗口中收集的数据做的计算操作，主要可以分为两类：
 * 		1.增量聚合函数（incremental aggregation functions）每条数据到来就进行计算，保持一个简单的状态。典型的增量聚合函数有
 * 			ReduceFunction, AggregateFunction。
 *		2.全窗口函数（full window functions）先把窗口所有数据收集起来，等到计算的时候会遍历所有数据。
 * 			ProcessWindowFunction 就是一个全窗口函数
 *
 */
public class Flink06_WindowFunction_IncrementalAggregation {

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

		// ReduceFunction 滚动窗口
		DataStream<Tuple2<String, Integer>> reduce = map.keyBy(0).timeWindow(Time.seconds(15))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
						return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
					}
				});

		// AggregateFunction 滑动窗口
		DataStream<Tuple2<String, Integer>> aggregate = map.keyBy(0).timeWindow(Time.seconds(15), Time.seconds(5))
				// 传入值   累加值   返回值
				.aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
					// 初始值
					@Override
					public Tuple2<String, Integer> createAccumulator() {
						return new Tuple2<>("", 0);
					}

					// 初始值 和 传入值 做累加
					@Override
					public Tuple2<String, Integer> add(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) {
						return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
					}

					@Override
					public Tuple2<String, Integer> getResult(Tuple2<String, Integer> v1) {
						return v1;
					}

					// 合并
					@Override
					public Tuple2<String, Integer> merge(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) {
						return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
					}
				});

		// ReduceFunction 滚动计数窗口
		DataStream<Tuple2<String, Integer>> count = map.keyBy(0).countWindow(10)
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
						return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
					}
				});


		count.print();


		//执行
		try {
			env.execute("demoTask1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
