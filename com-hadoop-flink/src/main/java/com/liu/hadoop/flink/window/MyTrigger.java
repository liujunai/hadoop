package com.liu.hadoop.flink.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author LiuJunFeng
 * @date 2021/4/27 下午11:03
 * @description:  自定义触发器  同时按照计数和时间（processing-time）触发计算
 */
public class MyTrigger extends Trigger<Tuple2<String,Integer>, TimeWindow> {

	@Override
	public TriggerResult onElement(Tuple2<String, Integer> v1, long l, TimeWindow timeWindow, TriggerContext context) throws Exception {
		return null;
	}

	@Override
	public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext context) throws Exception {
		return null;
	}

	@Override
	public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext context) throws Exception {
		return null;
	}

	@Override
	public void clear(TimeWindow timeWindow, TriggerContext context) throws Exception {

	}
}
