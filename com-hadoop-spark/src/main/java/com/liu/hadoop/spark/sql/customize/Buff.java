package com.liu.hadoop.spark.sql.customize;

import scala.Serializable;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 下午5:50
 * @description:   缓冲区类
 *
 *	要空参构造方法
 *	No applicable constructor/method found for zero actual parameters; candidates are: "com.liu.hadoop.spark.sql.customize.Buff(java.lang.Double, java.lang.Double)"
 *
 *	MyUDAF1 类跟 Buff 写在同一个文件，而spark需要public的访问权限
 *	No applicable constructor/method found for zero actual parameters; candidates are: "public long sparkSQL.Average.getCount()"
 *	1. 把  Buff 单独写成到一个java文件 Buff.java 里面是public访问权限
 * 	2. 把  MyUDAF1 “降级”跟 Buff 一起作为内部类
 *
 */
public class Buff  implements Serializable {

	public Buff() {
	}

	public Buff(Double ageTotal, Double count) {
		this.ageTotal = ageTotal;
		this.count = count;
	}

	private Double ageTotal;
	private Double count;

	public Double getAgeTotal() {
		return ageTotal;
	}

	public void setAgeTotal(Double ageTotal) {
		this.ageTotal = ageTotal;
	}

	public Double getCount() {
		return count;
	}

	public void setCount(Double count) {
		this.count = count;
	}
}