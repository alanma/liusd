package hbase.test;


import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.aspectj.org.eclipse.jdt.core.dom.ThisExpression;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-23
 */
public class GetTest {   
	static Configuration conf = HBaseConfiguration.create();
	public static Result getOneRow(HTable table,String rowkey) throws IOException{
		Get get = new Get(Bytes.toBytes(rowkey));
		return table.get(get);
	}
	public static void main(String[] args) throws IOException{
		HTable table = new HTable(conf, "cups_1");
		Get get = new Get(Bytes.toBytes("1212345678912342231"));
		
		Result rs = table.get(get);
		List<KeyValue> list = rs.list();
		System.out.println(list.size());
		for (KeyValue kv : list) {
			if( Bytes.toString(kv.getQualifier()).startsWith("settleAt")){
				System.out.println(Bytes.toLong( kv.getValue()));
			} else {
				System.out.println(Bytes.toString( kv.getValue())); 
			}   
		}
	}
}
