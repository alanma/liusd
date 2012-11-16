package hbase.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-9-7
 */
public class PageReader {
	public static void main(String[] args) throws IOException, InterruptedException{
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "cups_1");
		Scan scan = new Scan();
		scan.setCaching(5);
		int pageNum = 5;
		int count = 0;
		boolean flag =true;
		while(flag){
			int tmp = 0;
			ResultScanner rss = table.getScanner(scan);
			if(count > 0){
				rss.next(count);
			}
			while(tmp ++ <= pageNum){
				if(rss.iterator().hasNext()){
					Result rs = rss.next();
					if(rs!=null && rs.getRow()!=null){
						System.out.println(Bytes.toString(rs.getRow()));
					}
				}else{
					flag = false;
					break;
				}
			}
			System.out.println("\n 已读取: " +pageNum +"行。\n" );
			count += pageNum;
		}
	}
}
