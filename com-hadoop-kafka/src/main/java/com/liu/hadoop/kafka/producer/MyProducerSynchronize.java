package com.liu.hadoop.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author LiuJunFeng
 * @date 2021/4/9 下午4:51
 * @description:  同步发送 API 一条消息发送之后，会阻塞当前线程，直至返回 ack。
 *
 */
public class MyProducerSynchronize {

	public static void main(String[] args) throws ExecutionException,InterruptedException {

		Properties props = new Properties();
		props.put("bootstrap.servers", "hadoop102:9092");//kafka 集群，broker-list
		props.put("acks", "all");
		props.put("retries", 1);//重试次数
		props.put("batch.size", 16384);//批次大小
		props.put("linger.ms", 1);//等待时间
		props.put("buffer.memory", 33554432);//RecordAccumulator 缓冲区大小
		props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 100; i++) {
			producer.send(new ProducerRecord<String, String>("first",Integer.toString(i), Integer.toString(i))).get();
		}

		producer.close();
	}

}
