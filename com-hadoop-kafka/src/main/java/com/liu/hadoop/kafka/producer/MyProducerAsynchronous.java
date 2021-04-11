package com.liu.hadoop.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author LiuJunFeng
 * @date 2021/4/9 下午6:01
 * @description: 异步发送 API 不带回调函数的
 */
public class MyProducerAsynchronous {


	public static void main(String[] args) throws ExecutionException,InterruptedException {

		Properties props = new Properties();
		//kafka 集群，broker-list
		props.put("bootstrap.servers", "liu1:9092");
		//ACK应答级别
		props.put("acks", "all");
		//重试次数
		props.put("retries", 1);
		//批次大小
		props.put("batch.size", 16384);
		//等待时间
		props.put("linger.ms", 1);
		//RecordAccumulator 缓冲区大小
		props.put("buffer.memory", 33554432);
		//key value的序列化
		props.put("key.serializer",	"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 100; i++) {
			producer.send(new ProducerRecord<String, String>("first",	Integer.toString(i), Integer.toString(i)));
		}

		producer.close();
	}

}
