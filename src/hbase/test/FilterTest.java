package hbase.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-23
 */
public class FilterTest {
	/**
	 * 配置文件
	 */
	private static Configuration conf = HBaseConfiguration.create();
	/**
	 * @param args
	 * @throws IOException  
	 */
	public static void main(String[] args) throws IOException {
		HTable table = new HTable(conf, "table2");
		Scan scan = new  Scan();
		Filter filter = new RowFilter(CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("第一行")));
		scan.setFilter(filter);
		ResultScanner rss = table.getScanner(scan);
		for(Result rs:rss){
			System.out.println(Bytes.toString(rs.getRow()));
			
			System.out.println(rs);
		}
		rss.close();
	}

}
