package com.liu.hadoop.flink.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

/**
 * @author LiuJunFeng
 * @date 2021/4/27 下午11:19
 * @description:  自定义移除器
 */
public class MyEvictor implements Evictor<Tuple2<String,Integer>, TimeWindow> {

	@Override
	public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Integer>>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

	}

	@Override
	public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Integer>>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

	}
}
