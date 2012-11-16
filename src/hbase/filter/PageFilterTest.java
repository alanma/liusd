package hbase.filter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


/**
 * @author liusheding
 * @version 1.0
 * @create_date 2012-9-17
 */
public class PageFilterTest {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.writeXml(System.out);
		HTable table = new HTable(conf, "cups_1");
		Scan scan = new Scan();
		// Filter pageFilter = new PageFilter(20);
		// Filter prefixFilter = new PrefixFilter(Bytes.toBytes("1962"));
		// FilterList list = new FilterList();
		// list.addFilter(prefixFilter);
		// list.addFilter(pageFilter);
		// scan.setFilter(list);
		// scan.setStartRow(Bytes.toBytes("61"));
		// scan.setStopRow(Bytes.toBytes("62"));
		ResultScanner rss = table.getScanner(scan);
		System.out.println(System.nanoTime());
		long start = System.currentTimeMillis();
		long records = 0L;
		for (Result rs : rss) {
			// System.out.println(Bytes.toString(rs.getRow()));
			KeyValue[] list = rs.raw();
			for (KeyValue kv : list) {
				records++;
			}
			if (records >= 10000) {
				break;
			}
		}
		long end = System.currentTimeMillis();
		double elapsed = (end - start) / 1000.0;
		System.out.println(records + " records was found!  time elasped : "
				+ elapsed + " (s).");
		rss.close();
		
		table.close();
		System.out.println(System.nanoTime());
	}
}
