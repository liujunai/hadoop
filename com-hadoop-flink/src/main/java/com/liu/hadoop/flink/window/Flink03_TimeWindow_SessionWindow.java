package com.liu.hadoop.flink.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
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
 * Session Windows: 会话窗口
 * 		由一系列事件组合一个指定时间长度的 timeout 间隙组成，类似于 web 应用的session，也就是一段时间没有接收到新数据就会生成新的窗口。
 * 		特点：时间无对齐。
 * 		session 窗口分配器通过 session 活动来对元素进行分组，session 窗口跟滚动窗口和滑动窗口相比，不会有重叠和固定的开始时间和结束时间的情况，
 * 		相反，当它在一个固定的时间周期内不再收到元素，即非活动间隔产生，那个这个窗口就会关闭。一个 session 窗口通过一个 session 间隔来配置，
 * 		这个 session 间隔定义了非活跃周期的长度，当这个非活跃周期产生，那么当前的 session 将关闭并且后续的元素将被分配到新的 session 窗口中去
 *
 */
public class Flink03_TimeWindow_SessionWindow {

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
				// SessionWindows 没有简写 只能在window中指定
				.window(EventTimeSessionWindows.withGap(Time.minutes(1)));
//				.window(ProcessingTimeSessionWindows.withGap(Time.minutes(1)));
		


		//执行
		try {
			env.execute("demoTask1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
