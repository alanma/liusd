package test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.SortedMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.collections.map.LinkedMap;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-9-20
 */
public class MapTest<K,V extends Comparable<V>> {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MapTest a = new MapTest<String,Long>();
		SortedMap map = new ConcurrentSkipListMap(new MyComparetor());
		Map map1 = new TreeMap();
		map.put("1",1L);
		map.put("2",2L);
		a.sort(map);
		
	}
	public void sort(Map<K, V> map){
		Set set = map.entrySet();
		System.out.println(set.size());
		Iterator i =  set.iterator();
		while(i.hasNext()){
			Entry entry = (Entry) i.next();
			System.out.println(entry.getKey()+":"+entry.getKey());
		}
	}

	public static class MyComparetor implements Comparator<Long>{
		/**
		 * Long o1, Long o2
		 */
		public int compare(Long o1, Long o2) {
			
			return o1.compareTo(o2);
		}
		
	}

}
