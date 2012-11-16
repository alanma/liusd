package hbase.incr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <p>描述:  </p>
 *
 * <p>原则:   </p>
 *
 * <p>作用:   </p>
 *
 * <p>Create Time:  2012-11-15 <p>
 *
 * @author liusheding
 * @version 1.1
 */
public class IncrementPerformance {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		HTableInterface table = new HTable(conf, "m_summary_time");
		Increment inr = new Increment("testingperformance".getBytes());
		inr.addColumn("data".getBytes(), "count".getBytes(), 1L);
		int n = 10000;
		Get get = new Get("testingperformance".getBytes());
		System.out.println(Bytes.toLong(table.get(get).getValue("data".getBytes(), "count".getBytes())));
		long start = System.currentTimeMillis();
		/** for (int i = 0;i < n; i++) {
			table.increment(inr);
			if (i%1000 == 0) {
				System.out.println(i+" increments have been submitted.");
			}
		} **/
		List<Increment> inrs = new ArrayList<Increment>();
		int m = 1000;
		for (int i=0;i < n; i++) {
			inrs.add(inr);
			if (inrs.size() >= m) {
				table.batch(inrs);
				System.out.println(i+" increments have been submitted.");
				inrs.clear();
			}
		}
		System.out.println(Bytes.toLong(table.get(get).getValue("data".getBytes(), "count".getBytes())));
		table.close();
		long end = System.currentTimeMillis();
		System.out.println("time elasped is :" +(end - start)+" (ms).");
		//System.out.println(table.get(get).getValue("data".getBytes(), "count".getBytes()));
	}

}
