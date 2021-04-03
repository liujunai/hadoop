package com.liu.hadoop.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author LiuJunFeng
 * @date 2021/1/6 下午5:17
 * @description: 从kafka中读取数据
 */
public class SourceKafka {

	public static void main(String[] args) {

		//创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers","localhost:9595");

		//从kafka中读取数据
		DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>("sensor",new SimpleStringSchema(),properties));
		
		//输出数据
		dataStream.print("kafka输出");

		//执行
		try {
			env.execute("demoTask3");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


}
