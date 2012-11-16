package hbase.coprocessor;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.Callable;

import javax.imageio.ImageIO;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.AggregateImplementation;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author liusheding
 * @version 1.0
 * @create_date 2012-9-3
 */
public class CardNumSum {
	/**
	 * 最小银行卡前缀
	 */
	public final static String MIN_CARD_NO = "160000000000000000000";
	/**
	 * 最大银行卡前缀
	 */
	public final static String MAX_CARD_NO = "199999999999999999999";

	public final static String MCHNT_CD = "102372350393004";
	public static final String[] month = { "01", "02", "03", "04", "05", "06",
		"07", "08", "09", "10", "11", "12", "13" };
	/**
	 * @param args
	 * @throws Throwable
	 */
	public static void main(String[] args) throws Throwable {
		Configuration conf = HBaseConfiguration.create();
		// HTable table = new HTable(conf, "cups_test_0");
		AggregationClient client = new AggregationClient(conf);
		Scan scan = new Scan();
		
		// SingleColumnValueFilter filter = new SingleColumnValueFilter(
		// Bytes.toBytes("data"), Bytees.toBytes("mchnt_cd"),
		// CompareOp.EQUAL, Bytes.toBytes(MCHNT_CD));
		// scan.addColumn(Bytes.toBytes("data"));
		// scan.addFamily(Bytes.toBytes("data"));
		// scan.addColumn(, qualifier);
		// scan.setFilter(filter);
		QualifierFilter filter = new QualifierFilter(CompareOp.EQUAL, new SubstringComparator("settleAt2012"));
	
		ValueFilter v = new ValueFilter(CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(100000L)));
		
		FilterList list = new FilterList();
		
		list.addFilter(filter);
		
		list.addFilter(v);
		
		scan.setCaching(1000);
		
		scan.addFamily(Bytes.toBytes("data"));
		
		scan.setFilter(list);
		
		scan.setStartRow(Bytes.toBytes("1412345678912345202"));
		
		scan.setStopRow(Bytes.toBytes("1412345678912345450"));
		//scan.addColumn(Bytes.toBytes("index"), Bytes.toBytes("settleAt"));
		
		final ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();
		/**
		for (int i = 0; i < 12; i++) {
			scan.setStartRow(Bytes.toBytes("8984301599824172010"+month[i]));
			scan.setStopRow(Bytes.toBytes("8984301599824172010"+month[i]+"31"));
			Long sum = client.sum(Bytes.toBytes("index_merchant_cups_6"), ci,
					scan);
			// long cout = client.rowCount(Bytes.toBytes("cups_test_0"), ci,
			// scan);
			System.out.println(sum);
		}**/
		long start = System.currentTimeMillis();
		long rowCount = client.rowCount(Bytes.toBytes("cups_1"), ci, scan);
		//long sum = client.sum(Bytes.toBytes("cups_1"), ci, scan);
		long end = System.currentTimeMillis();
		//FileOutputStream out = new FileOutputStream("/home/liusd/2.txt");
		//out.write(("total money is :" + sum + "(Y), time elapsed is "+ (end - start)/1000 + "(s).").getBytes());
		System.out.println("total money is :" + rowCount + "(Y), time elapsed is "+ (end - start)/1000 + "(s).");
	}

}
