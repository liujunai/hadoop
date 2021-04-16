package com.liu.hadoop.spark.util;

import java.util.*;

/**
 * @author LiuJunFeng
 * @date 2021/4/15 下午4:25
 * @description:
 */
public class SortMap {

	/**
	 * @author LiuJunFeng
	 * @date 2021/4/15 下午4:27
	 * @Description:  Map的value值降序排序
	 * @return java.util.Map
	*/
	public Map<String, Integer> sortMap(Map<String, Integer> map) {

		//获取entrySet
		Set<Map.Entry<String, Integer>> mapEntries = map.entrySet();

		//使用链表来对集合进行排序，使用LinkedList，利于插入元素
		List<Map.Entry<String, Integer>> result = new LinkedList<>(mapEntries);
		//自定义比较器来比较链表中的元素
		Collections.sort(result, new Comparator<Map.Entry<String, Integer>>() {
			//基于entry的值（Entry.getValue()），来排序链表
			@Override
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		//将排好序的存入到LinkedHashMap(可保持顺序)中，需要存储键和值信息对到新的映射中。
		Integer sort = 1;
		Map<String, Integer> linkMap = new LinkedHashMap<>();
		for (Map.Entry<String, Integer> newEntry : result) {
			// 取出排名前5的值
			if (sort <= 3) {
				linkMap.put(newEntry.getKey(), newEntry.getValue());
				++sort;
			}
		}
		return linkMap;
	}

}
