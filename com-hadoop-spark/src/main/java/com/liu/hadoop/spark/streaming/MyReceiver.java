package com.liu.hadoop.spark.streaming;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * @author LiuJunFeng
 * @date 2021/4/20 下午11:14
 * @description: 自定义数据源
 * <p>
 * 自定义数据采集器
 * 1. 继承Receiver，定义泛型, 传递参数
 * 2. 重写方法
 */
public class MyReceiver extends Receiver<String> {

	private boolean flg = true;
	private String host = null;
	private int port = -1;

	public MyReceiver(StorageLevel storageLevel, String host_, int port_) {
		super(storageLevel);
		this.host = host_;
		this.port = port_;
	}

	@Override
	public void onStart() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (flg) {
					String userInput = null;
					try {
						Socket socket = new Socket(host, port);
						BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

						while (!isStopped() && (userInput = reader.readLine()) != null) {
							store(userInput);
						}
						reader.close();
						socket.close();

					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}).start();

	}

	@Override
	public void onStop() {
		flg = false;
	}
}
