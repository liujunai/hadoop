package com.liu.hadoop.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * @author LiuJunFeng
 * @date 2021/4/9 下午6:41
 * @description: 手动提交 offset 异步提交 offset
 * 虽然同步提交 offset 更可靠一些，但是由于其会阻塞当前线程，直到提交成功 因此吞吐量会收到很大的影响
 * commitAsync 则没有失败重试机制，故有可能提交失败
 */
public class MyConsumerAsynchronous {

	public static void main(String[] args) {

		Properties props = new Properties();
		//Kafka 集群
		props.put("bootstrap.servers", "liu1:9092");
		//消费者组，只要 group.id 相同，就属于同一个消费者组
		props.put("group.id", "test");
		//关闭自动提交 offset
		props.put("enable.auto.commit", "false");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		//消费者订阅主题
		consumer.subscribe(Arrays.asList("first"));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);//消费者拉取数据

			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
			}
			//异步提交
			consumer.commitAsync(new OffsetCommitCallback() {
				@Override
				public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
					if (exception != null) {
						System.err.println("Commit failed for" + offsets);
					}
				}
			});
		}
	}


}
