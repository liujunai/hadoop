package com.liu.hadoop.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @author LiuJunFeng
 * @date 2021/4/9 下午6:06
 * @description:  自动提交 offset
 */
public class MyConsumer {

	public static void main(String[] args) {

		Properties props = new Properties();

		props.put("bootstrap.servers", "liu1:9092");
		//消费者组
		props.put("group.id", "test");
		//开启自动提交 offset
		props.put("enable.auto.commit", "true");
		//自动提交延迟
		props.put("auto.commit.interval.ms", "1000");

		props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",	"org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList("first"));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		}

	}

}
