package com.liu.hadoop.spark.streaming.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

/**
 * @author LiuJunFeng
 * @date 2021/4/22 下午12:48
 * @description: 生成模拟数据
 */
public class Spark01_SparkStreaming_MockData {

	public static void main(String[] args) {

		// kafka生产数据
		Properties prop = new Properties();
		prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "liu1:9092,liu2:9092,liu3:9092");
		prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(prop);

		while (true) {

			List<String> list = new ArrayList<>();
			List<String> areaList = Arrays.asList("华中", "华北", "华南");
			List<String> cityList = Arrays.asList("武汉", "北京", "南京");

			for (int x = 0; x < 10; x++) {
				String area = areaList.get(new Random().nextInt(3));
				String city = cityList.get(new Random().nextInt(3));
				int userid = new Random().nextInt(6) + 1;
				int adid = new Random().nextInt(6) + 1;

				list.add(System.currentTimeMillis() + "," + area + "," + city + "," + userid + "," + adid);
			}

			for (String s : list) {
				ProducerRecord<String, String> producer = new ProducerRecord<>("liu", s);
				kafkaProducer.send(producer);
				System.out.println("producer = " + s);
			}

			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}


}
