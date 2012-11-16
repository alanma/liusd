package hbase.filter;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <p>描述:  </p>
 *
 * <p>原则:   </p>
 *
 * <p>作用:   </p>
 *
 * <p>Create Time:  2012-11-2 <p>
 *
 * @author liusheding
 * @version 1.1
 */
public class CupsFilterTest {
	
	public static void main(String[] args) throws IOException, SecurityException, NoSuchFieldException {
		Configuration conf = HBaseConfiguration.create();
		HTableInterface table = new HTable(conf, "cups_1");
		Scan scan = new Scan();
		ResultScanner rss = table.getScanner(scan);
		int i = 0; 
		LOOP: for (Result rs:rss){
			for(KeyValue kv:rs.raw()) {
				System.out.println(Bytes.toString(kv.getBuffer(),kv.getValueOffset(),kv.getValueLength()));
				if (i++ > 100) {
					break LOOP;
				}
			}
		}
		System.out.println("name");
		rss.close();
		table.close();
	}

}
