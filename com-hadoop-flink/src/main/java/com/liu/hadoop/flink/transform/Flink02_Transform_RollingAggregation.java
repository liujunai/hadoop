package com.liu.hadoop.flink.transform;

import com.liu.hadoop.flink.beans.Sensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description:  Transform  滚动聚合算子  基于Key的分组转换  Rolling Aggregation
 *
 * 这些算子可以针对 KeyedStream 的每一个支流做聚合。
 * sum() 求和
 * min() 获取最大值
 * max() 获取最小值
 * minBy() 获取当前数据一整条最大值
 * maxBy() 获取当前数据一整条最小值
 * reduce() 选取所需要的值
 *
 * KeyBy:
 * DataStream → KeyedStream：逻辑地将一个流拆分成不相交的分区，每个分区包含具有相同 key 的元素，在内部以 hash 的形式实现的
 *
 * KeyedStream → DataStream：一个分组数据流的聚合操作，合并当前的元素和上次聚合的结果，产生一个新的值，返回的流中包含每一次聚合
 * 的结果，而不是只返回最后一次聚合的最终结果。
 *
 */
public class Flink02_Transform_RollingAggregation {

	public static void main(String[] args) {

		//创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		// 从文件读取数据
		DataStream<String> dataStream = env.readTextFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-flink/data/sensor.txt");

		// 数据转换
		SingleOutputStreamOperator<Sensor> sensorMap = dataStream.map(new MapFunction<String, Sensor>() {
			@Override
			public Sensor map(String s) throws Exception {
				String[] s1 = s.split(",");
				return new Sensor(s1[0], new Long(s1[1]), new Double(s1[2]));
			}
		});

		// 数据分组
		KeyedStream<Sensor, Tuple> keyedStream = sensorMap.keyBy("id");

		// 分组统计
		DataStream<Sensor> max1 = keyedStream.max("temperature");

		DataStream<Sensor> max2 = keyedStream.maxBy("temperature");

		// reduce聚合，取最大的温度值，以及当前最新的时间戳
		SingleOutputStreamOperator<Sensor> max3 = keyedStream.reduce(new ReduceFunction<Sensor>() {
			@Override
			public Sensor reduce(Sensor v1, Sensor v2) throws Exception {
				return new Sensor(v1.getId(), v2.getTimestamp(), Math.max(v1.getTemperature(), v2.getTemperature()));
			}
		});


		max2.print();


		//执行
		try {
			env.execute("demoTask1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
