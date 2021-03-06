package com.liu.hadoop.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author LiuJunFeng
 * @date 2021/4/9 下午6:02
 * @description:  异步发送 API 带回调函数的
 */
public class MyProducerAsynchronous1 {

	public static void main(String[] args) throws ExecutionException,InterruptedException {

		Properties props = new Properties();
		props.put("bootstrap.servers", "hadoop102:9092");//kafka 集群，broker-list
		props.put("acks", "all");
		props.put("retries", 1);//重试次数
		props.put("batch.size", 16384);//批次大小
		props.put("linger.ms", 1);//等待时间
		props.put("buffer.memory", 33554432);//RecordAccumulator 缓冲区大小
		props.put("key.serializer",	"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 100; i++) {
			producer.send(new ProducerRecord<String, String>("first",Integer.toString(i), Integer.toString(i)), new Callback() {
				//回调函数，该方法会在 Producer 收到 ack 时调用，为异步调用
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception == null) {
						System.out.println("success->" +metadata.offset());
					} else {
						exception.printStackTrace();
					}
				}
			});
		}

		producer.close();
	}

}
