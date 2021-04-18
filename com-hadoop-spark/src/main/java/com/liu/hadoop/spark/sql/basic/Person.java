package com.liu.hadoop.spark.sql.basic;

import scala.Serializable;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午3:24
 * @description:
 */
public class Person {

	private String id;
	private String name;
	private String age;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}
}
