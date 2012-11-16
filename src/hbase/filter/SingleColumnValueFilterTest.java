package hbase.filter;

import hbase.coprocessor.CardNumSum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.aspectj.util.FuzzyBoolean;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-23
 */
public class SingleColumnValueFilterTest {

	public static Configuration conf = HBaseConfiguration.create();


	public static void main(String[] args) throws IOException{
		conf.addResource("/conf");
		HTable table= new HTable(conf, "cups_detail");
		/*SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("data"),
				Bytes.toBytes("mchnt_cd"), 
				CompareOp.EQUAL, 
				Bytes.toBytes(CardNumSum.MCHNT_CD));
		*/
		//filter.setFilterIfMissing(true);
		
		Get get = new  Get(Bytes.toBytes("195612345678912347785102372350393004201109161409316852"));
		
		//Scan scan = new Scan(Bytes.toBytes("195612345678912347785102372350393004201109161409316852"));
		/**scan.setStartRow(Bytes.toBytes("195612345678912347785"));
		scan.setStopRow((Bytes.toBytes("195612345678912347785"));*/
		//scan.setFilter(filter);
		//scan.addColumn(Bytes.toBytes("data"),Bytes.toBytes("settle_at"));
		//ResultScanner rss = table.getScanner(scan);
		Result rs1 = table.get(get);
		int i = 0;
		//for(Result rs : rss){
			//i++;
			//System.out.println(rs);
		//}
		System.out.println(Bytes.toString(rs1.getRow()));
		System.out.println(Bytes.toLong(rs1.getColumnLatest(Bytes.toBytes("data"),Bytes.toBytes("settle_at")).getValue()));
		//System.out.println(Bytes.toLong( rs1.getColumn(Bytes.toBytes("data"),Bytes.toBytes("settle_at")).get(0).getValue()));
		System.out.println(i+" records was found!");
		try {
			table.coprocessorExec(null, null, null,null);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		table.close();
	}
	
}
