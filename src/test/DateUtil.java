package test;

import java.util.Date;


/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-21
 */
public class DateUtil {
	public static Date getDateByTimestamp(long timestamp){
		
		return new Date(timestamp);
	
	}
	
	public static void main(String[] args){
		System.out.println(getDateByTimestamp(1345571702273L).toGMTString());
		long num = 122432434L;
		double a = ((double)num)/100;
		System.out.println(a);
	}
	
}
