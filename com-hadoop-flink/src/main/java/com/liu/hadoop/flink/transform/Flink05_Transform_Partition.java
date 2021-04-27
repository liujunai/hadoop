package com.liu.hadoop.flink.transform;

import com.liu.hadoop.flink.beans.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description:  Transform   Partition   数据重分布转换
 *
 * 默认情况下，数据是自动分配到多个实例上的。有的时候，我们需要手动对数据在多个实例上进行分配，例如，我们知道某个实例上的数据过多，
 * 其他实例上的数据稀疏，产生了数据倾斜，这时我们需要将数据均匀分布到各个实例上，以避免部分实例负载过重。数据倾斜问题会导致整个作
 * 业的计算时间过长或者内存不足等问题
 *
 * shuffle: 基于正态分布，将数据随机分配到下游各算子实例上。
 *
 * rebalance: 将数据均匀分配到各实例上,上游的数据会轮询式地分配到下游的所有的实例上
 *
 * rescale: 将数据均匀分布到各下游各实例上，但它的传输开销更小，因为rescale并不是将每个数据轮询地发送给下游每个实例，而是就近发送给下游实例。
 *
 * broadcast: 数据会被复制并广播发送给下游的所有实例上
 *
 * global: 会所有数据发送给下游算子的第一个实例上，使用这个算子时要小心，以免造成严重的性能问题。
 *
 * forward: 输出到下游本地的实例,分区器要求上下游算子并行度一样
 *
 * partitionCustom: 自定义分区器
 */
public class Flink05_Transform_Partition {

	public static void main(String[] args) {

		//创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		// 从文件读取数据
		DataStream<String> dataStream = env.readTextFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-flink/data/sensor.txt");

		// 数据转换
		DataStream<Sensor> sensorMap = dataStream.map(new MapFunction<String, Sensor>() {
			@Override
			public Sensor map(String s) throws Exception {
				String[] split = s.split(",");
				return new Sensor(split[0],new Long(split[1]),new Double(split[2]));
			}
		});

		sensorMap.rebalance();

		sensorMap.print();

		//执行
		try {
			env.execute("demoTask1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


}
