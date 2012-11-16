package hbase.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-28
 */
public class ScanTest {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTableInterface table = new HTable(conf, "cups_1");
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("data"));
		Filter filter = new ColumnPrefixFilter(Bytes.toBytes("2012"));
		scan.setFilter(filter);
		ResultScanner rss = table.getScanner(scan);
		int i = 0;
		for(Result rs :rss){
			System.out.println(Bytes.toString(rs.getRow())+":");
			for (KeyValue kv : rs.raw()) {
				if (Bytes.toString(kv.getQualifier()).startsWith("settleAt")) {
					System.out.println(Bytes.toLong(kv.getValue()));
				} else {
					System.out.println(Bytes.toString(kv.getValue()));
				}
			}
			System.out.print("\n");
			i++;
			if (i>100) {
				break;
			}
		}
		rss.close();
		table.close();
	}

}
