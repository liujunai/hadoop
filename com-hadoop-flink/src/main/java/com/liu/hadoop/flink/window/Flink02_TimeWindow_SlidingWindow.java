package com.liu.hadoop.flink.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description: TimeWindow：按照时间生成 Window。
 *
 * TimeWindow 是将指定时间范围内的所有数据组成一个 window，一次对一个window 里面的所有数据进行计算。
 * 			  默认的时间窗口根据 Processing Time（处理时间） 进行窗口的划分，将 Flink 获取到的数据根据进入 Flink 的时间划分到不同的窗口中
 *
 * 对于 TimeWindow，可以根据窗口实现原理的不同分成三类：滚动窗口（Tumbling Window）、滑动窗口（Sliding Window）和会话窗口（Session Window）。
 *
 * Sliding Windows: 滑动窗口
 * 		滑动窗口是固定窗口的更广义的一种形式，滑动窗口由固定的窗口长度和滑动间隔组成。
 * 		特点：时间对齐，窗口长度固定，可以有重叠。
 * 		滑动窗口分配器将元素分配到固定长度的窗口中，与滚动窗口类似，窗口的大小由窗口大小参数来配置，另一个窗口滑动参数控制滑动窗口开始的频率。
 * 		因此，滑动窗口如果滑动参数小于窗口大小的话，窗口是可以重叠的，在这种情况下元素会被分配到多个窗口中。
 * 	滑动窗口和滚动窗口的函数名是完全一致的，只是在传参数时需要传入两个参数，一个是 window_size，一个是 sliding_size
 *
 */
public class Flink02_TimeWindow_SlidingWindow {

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

		// 直接基于 map 开窗 可以调用windowAll 会将所有数据放入第一个分区中 相当于global
//		map.windowAll()

		map.keyBy(0)
				// 可以直接 timeWindow 根据参数指定 窗口：滚动、滑动    时间语意：在内部判断
				.timeWindow(Time.seconds(15),Time.seconds(5));
				// 可以使用 window 直接指定
				// 窗口：滚动、滑动
				// 时间语意： (ProcessingTime)处理时间、(EventTime)事件时间
//				.window(TumblingEventTimeWindows.of(Time.seconds(15)));
//				.window(SlidingEventTimeWindows.of(Time.minutes(15),Time.seconds(5)));
		


		//执行
		try {
			env.execute("demoTask1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
