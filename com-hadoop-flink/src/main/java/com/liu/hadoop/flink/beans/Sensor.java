package com.liu.hadoop.flink.beans;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description: 温度类型
 */
public class Sensor {

	private String id;
	private Long timestamp;
	private Double temperature;

	public Sensor() {
	}

	public Sensor(String id, Long timestamp, Double temperature) {
		this.id = id;
		this.timestamp = timestamp;
		this.temperature = temperature;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Double getTemperature() {
		return temperature;
	}

	public void setTemperature(Double temperature) {
		this.temperature = temperature;
	}

	@Override
	public String toString() {
		return "Sensor{" +
				"id='" + id + '\'' +
				", timestamp=" + timestamp +
				", temperature=" + temperature +
				'}';
	}
}
