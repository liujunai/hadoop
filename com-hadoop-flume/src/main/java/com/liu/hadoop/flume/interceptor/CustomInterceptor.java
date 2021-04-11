package com.liu.hadoop.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/5 下午5:25
 * @description: 自定义 Interceptor
 *
 * 绑定自定义的 Interceptor
 * a1.sources.r1.interceptors = i1
 * a1.sources.r1.interceptors.i1.type = com.atguigu.flume.interceptor.CustomInterceptor$Builder
 * a1.sources.r1.selector.type = multiplexing
 * a1.sources.r1.selector.header = type
 * a1.sources.r1.selector.mapping.letter = c1
 * a1.sources.r1.selector.mapping.number = c2
 *
 *
 *
 */
public class CustomInterceptor implements Interceptor {


	public void initialize() {

	}

	public Event intercept(Event event) {
		byte[] body = event.getBody();
		if (body[0] < 'z' && body[0] > 'a') {
			event.getHeaders().put("type", "letter");
		} else if (body[0] > '0' && body[0] < '9') {
			event.getHeaders().put("type", "number");
		}
		return event;
	}

	public List<Event> intercept(List<Event> events) {
		for (Event event : events) {
			intercept(event);
		}
		return events;
	}

	public void close() {

	}

	public static class Builder implements Interceptor.Builder {

		public Interceptor build() {
			return new CustomInterceptor();
		}

		public void configure(Context context) {
		}

	}


}
