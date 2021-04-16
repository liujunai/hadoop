package com.liu.hadoop.spark.core.rdd.serialization;


import scala.Serializable;

/**
 * @author LiuJunFeng
 * @date 2021/4/16 上午1:41
 * @description:  使用的是 scala.Serializable 的接口
 */
public class User implements Serializable {

	private Integer age = 30;


	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}
}
