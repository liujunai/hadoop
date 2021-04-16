package com.liu.hadoop.spark.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/14 下午8:41
 * @description:  时间转换
 */
public class DateConversion {

	public static void main(String[] args) throws ParseException {

		String str = "20/05/2015:20:05:33";
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
		Date date = dateFormat.parse(str);

		Calendar cal = Calendar.getInstance();
		cal.setTime(date);//指定日期
		cal.getTime();//获取日期

		int year = cal.get(Calendar.YEAR);//获取年份
		int month = cal.get(Calendar.MONTH) + 1;//获取月份
		int day = cal.get(Calendar.DATE);//获取日
//		int hour = cal.get(Calendar.HOUR);// 12小时 制
		int hour = cal.get(Calendar.HOUR_OF_DAY);//24小时 制
		int minute = cal.get(Calendar.MINUTE);//分
		int second = cal.get(Calendar.SECOND);//秒
		int WeekOfYear = cal.get(Calendar.DAY_OF_WEEK);//一周的第几天
		System.out.println("现在的时间是：公元" + year + "年" + month + "月" + day + "日" + hour + "时" + minute + "分"
				+ second + "秒  星期" + WeekOfYear);


	}


}
