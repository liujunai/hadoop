package com.liu.hadoop.flink.transform;

import com.liu.hadoop.flink.beans.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;
import java.util.Collections;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description:  Transform  多数据流转换
 * Split 和 Select
 * Split:  DataStream --> SplitStream：根据某些特征把一个 DataStream 拆分成两个或者多个 DataStream。
 * Select： SplitStream --> DataStream：从一个 SplitStream 中获取一个或者多个DataStream。
 *
 * Connect 和 CoMap
 * Connect: DataStream --> ConnectedStreams：连接两个保持他们类型的数据流，两个数据流被 Connect 之后，只是被放在了一个同一个流中，
 * 内部依然保持各自的数据和形式不发生任何变化，两个流相互独立。
 * CoMap: ConnectedStreams --> DataStream：作用于 ConnectedStreams 上，功能与 map和 flatMap 一样，对 ConnectedStreams 中的
 * 每一个 Stream 分别进行 map 和 flatMap处理。
 *
 * Union : DataStream → DataStream：对两个或者两个以上的 DataStream 进行 union 操作，产生一个包含所有 DataStream 元素的新 DataStream。
 *
 * Connect 与 Union 区别：
 * 		1． Union 之前两个流的类型必须是一样，Connect 可以不一样，在之后的 coMap中再去调整成为一样的。
 * 		2. Connect 只能操作两个流，Union 可以操作多个。
 *
 */
public class Flink03_Transform_MultipleStreams {

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

		// 1.Split 分流
		SplitStream<Sensor> splitStream = sensorMap.split(new OutputSelector<Sensor>() {
			@Override
			public Iterable<String> select(Sensor sensor) {
				return (sensor.getTemperature() > 30) ? Collections.singletonList("gao") : Collections.singletonList("di");
			}
		});

		DataStream<Sensor> highSensor = splitStream.select("gao");
		DataStream<Sensor> lowSensor = splitStream.select("di");

//		highSensor.print("gao");
//		lowSensor.print("di");

		// 2.connect 合流
		DataStream<Tuple2<String, Double>> highConnect = highSensor.map(new MapFunction<Sensor, Tuple2<String, Double>>() {
			@Override
			public Tuple2<String, Double> map(Sensor sensor) throws Exception {
				return new Tuple2<>(sensor.getId(), sensor.getTemperature());
			}
		});

		ConnectedStreams<Tuple2<String, Double>, Sensor> connect = highConnect.connect(lowSensor);

		DataStream<Object> map = connect.map(new CoMapFunction<Tuple2<String, Double>, Sensor, Object>() {
			@Override
			public Object map1(Tuple2<String, Double> v1) throws Exception {
				return new Tuple3<>(v1.f0, v1.f1, "这是高温流输出");
			}

			@Override
			public Object map2(Sensor sensor) throws Exception {
				return new Tuple2<>(sensor.getId(), "这是低温流输出");
			}
		});

//		map.print();

		// 3.Union 合流
		DataStream<Sensor> union = highSensor.union(lowSensor);
		union.print();


		//执行
		try {
			env.execute("demoTask1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
