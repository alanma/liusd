package util;

import hbase.mapreduce.CardgradeSummary;
import hbase.mapreduce.CardgradeSummary.Type;

import java.io.UnsupportedEncodingException;

import org.jruby.RubyProcess.Sys;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-28
 */
public class StringUtil {

	
	/**
	 * @param args
	 * @throws UnsupportedEncodingException 
	 */
	public static void main(String[] args) throws UnsupportedEncodingException {
		String s = "583	8888	匡佐强银行	借记卡	3	3	000	333	白金卡";
		String s1 = "dasdsad   dasd   dasd   d dasd";
		String[] a = s.split("\\	 {0,9}");
		for(int i= 0 ;i<a.length;i++){
			System.out.println(a[i]);
		}
		Integer asad = 2;
		System.out.println(asad instanceof Integer);
		System.out.println(Type.hadoopFile.toString() +12);
		
		System.out.println("195612345678912347956914372351310052201201102001293415".substring(40,42));
		System.out.println(Integer.parseInt("12"));
		long[] asadz = new long[10];
		
		for(int i = 0;i<asadz.length;i++){
			System.out.println(asadz[i]);
		}
		System.out.println(System.getenv());
		System.out.println(System.getenv("HADOOP_USER_NAME"));
		System.out.println(s instanceof Object);
		System.out.println("20121213das".substring(0, 8));
		System.out.println("a".compareTo("b"));
	}

}
