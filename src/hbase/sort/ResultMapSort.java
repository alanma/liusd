package hbase.sort;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class ResultMapSort {
	
	public static Map<String,Long> sortByValue(Map<String,Long> map){
		List<Map.Entry<String, Long>> list = new LinkedList<Map.Entry<String,Long>>(map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
			public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
				return -o1.getValue().compareTo(o2.getValue());
			}
		});
		Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();
		Iterator<Map.Entry<String, Long>> iterator = list.iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, Long> entry = iterator.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return map;
	}
	
}
