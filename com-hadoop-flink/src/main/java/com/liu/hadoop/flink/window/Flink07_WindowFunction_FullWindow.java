package com.liu.hadoop.flink.window;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;

import java.util.Iterator;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description: window function：窗口函数  -->全窗口函数
 *
 * window function 定义了要对窗口中收集的数据做的计算操作，主要可以分为两类：
 * 		1.增量聚合函数（incremental aggregation functions）每条数据到来就进行计算，保持一个简单的状态。典型的增量聚合函数有
 * 			ReduceFunction, AggregateFunction。
 *		2.全窗口函数（full window functions）先把窗口所有数据收集起来，等到计算的时候会遍历所有数据。
 * 			ProcessWindowFunction     WindowFunction
 *
 */
public class Flink07_WindowFunction_FullWindow {

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

		// WindowFunction 滚动窗口
		DataStream<Tuple3<String, Long, Integer>> windowData = map.keyBy(0).timeWindow(Time.seconds(15))
				.apply(new WindowFunction<Tuple2<String, Integer>, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
					// 	1.分组的key 2.timeWindow方法 3.传入值 4.返回值
					@Override
					public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
						// 获取window 执行结束时间
						long end = timeWindow.getEnd();

						String field = tuple.getField(0);
						int size = IteratorUtils.toList(iterable.iterator()).size();
						collector.collect(new Tuple3<>(field,end,size));
					}
				});

		// ProcessWindowFunction 滑动窗口
		// ProcessWindowFunction中包含 context 上下文 在context 中包含timeWindow 可以获取到信息更多
		DataStream<Tuple3<String, Long, Integer>> processWindowData = map.keyBy(0).timeWindow(Time.seconds(15), Time.seconds(5))
				.process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
					@Override
					public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
						// 获取window 执行结束时间
						long end = context.window().getEnd();

						String field = tuple.getField(0);
						int size = IteratorUtils.toList(iterable.iterator()).size();
						collector.collect(new Tuple3<>(field,end,size));
					}
				});

		// WindowFunction 滑动计数窗口
		DataStream<Tuple3<String, Long,Integer>> count = map.keyBy(0).countWindow(10,2)
				.apply(new WindowFunction<Tuple2<String, Integer>, Tuple3<String, Long,Integer>, Tuple, GlobalWindow>() {
					@Override
					public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple3<String, Long,Integer>> collector) throws Exception {
						// 获取window 执行结束时间
						long end = globalWindow.maxTimestamp();

						String field = tuple.getField(0);
						int size = IteratorUtils.toList(iterable.iterator()).size();
						collector.collect(new Tuple3<>(field,end,size));
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
