package com.liu.hadoop.flink.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description:  CountWindow ：按照指定的数据条数生成一个 Window，与时间无关，默认的 CountWindow 是一个滚动计数窗口。
 *
 * 对于 CountWindow，可以根据窗口实现原理的不同分成二类：滚动计数窗口（Tumbling window）和 滑动计数窗口（Sliding window）
 *
 * CountWindow 根据窗口中相同 key 元素的数量来触发执行，执行时只计算元素数量达到窗口大小的 key 对应的结果,不是输入的所有元素的总数
 *
 * Tumbling window: 滚动计数窗口
 * 		只需要指定窗口大小即可，当元素数量达到窗口大小时，就会触发窗口的执行
 *
 */
public class Flink04_CountWindow_TumblingWindow {

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

		map.keyBy(0).countWindow(10);



		//执行
		try {
			env.execute("demoTask1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
