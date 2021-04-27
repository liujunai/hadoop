package com.liu.hadoop.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author LiuJunFeng
 * @date 2021/1/6 下午5:17
 * @description: 从kafka中读取数据
 *
 * 需要引入 kafka 连接器的依赖
 */
public class Flink03_Source_Kafka {

	public static void main(String[] args) {

		//创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// kafka 消费者参数
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "liu1:9092,liu2:9092,liu3:9092");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink");

		//从kafka中读取数据
		DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>("liu",new SimpleStringSchema(),properties));
		
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
