package com.liu.hadoop.flink.sink;

import com.liu.hadoop.flink.beans.Sensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description:  将结果发送 kafka
 *
 * Flink 没有类似于 spark 中 foreach 方法，让用户进行迭代的操作。虽有对外的输出操作都要利用 Sink 完成
 */
public class Flink01_Sink_Kafka {

	public static void main(String[] args) {

		//创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//全局并行度
		env.setParallelism(1);

		//从文件中读取数据
		DataStream<String> dataStream = env.readTextFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-flink/data/sensor.txt");


		// kafka 生产者
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"liu1:9092,liu2:9092,liu3:9092");

		dataStream.addSink(new FlinkKafkaProducer011<String>("liu",new SimpleStringSchema(),properties));


		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		//执行
		try {
			env.execute("demoTask1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
