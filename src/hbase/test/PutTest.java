package hbase.test;

import hadoop.jdbc.model.CupsDetail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-22
 */
public class PutTest {

	/**TODO Auto-generated method stub
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTableInterface table = new HTable(conf, "summary_time");
		/**
			Put put = new Put(Bytes.toBytes("第一行"));
			put.add(toByte("col2"),toByte("id"),toByte("1"));
			put.add(toByte("col2"),toByte("name"),toByte("liusheding"));
			table.put(put);
		*/
		//HBaseAdmin admin = new HBaseAdmin(conf);
		/**
			admin.disableTable("table2");
			admin.addColumn("table2", new HColumnDescriptor("col1"));
			admin.enableTable("table2");
		*/
		Increment inr = new Increment("20121112".getBytes());
		inr.addColumn("data".getBytes(),"sum".getBytes(), 1L);
		table.increment(inr);
		table.close();
	}
	/**
	 * 
	 * @param str
	 * @return
	 */
	public static byte[] toByte(String str){
		return Bytes.toBytes(str);
	}
	
}
