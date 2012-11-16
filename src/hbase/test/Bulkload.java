package hbase.test;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-23
 */
public class Bulkload{
	HFileOutputFormat hout;
	private static class BulkloadMapper {
		public static void main(String[] args){
			byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes("data:json"));	
			System.out.println(colkey.length);
			System.out.println(Bytes.toString( colkey[0])+"    "+Bytes.toString( colkey[1]));
			final Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes("122"));
		}
	}
}
