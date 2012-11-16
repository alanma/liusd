package util;

import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-27
 */
public class DateUtil {
	
	public static void main(String[] args){
		System.out.println(new Date(9l).toLocaleString());
		System.out.println("liusd".substring(1));
		String[] s = {"a","b"};
		System.out.println();
		System.out.println(System.currentTimeMillis());
		int year = Calendar.getInstance().get(Calendar.YEAR);
		int month = Calendar.getInstance().get(Calendar.MONTH);
		Date date = Calendar.getInstance().getTime();
		System.out.println(date);
		System.out.println(year);
		System.out.println(System.getProperty("os.name")+System.getProperty("os.version"));
		System.out.println(System.getProperty("java.io.tmpdir"));
		
		Hashtable hashtable = System.getProperties();
		
		Iterator iterator = hashtable.entrySet().iterator();
		
		
		
		while (iterator.hasNext()){
			
			@SuppressWarnings("unchecked")
			Entry<String, String> entry = (Entry<String,String>) iterator.next();
			
			System.out.println(entry);
		}
	}
}
