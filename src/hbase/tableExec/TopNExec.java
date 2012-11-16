package hbase.tableExec;

import hbase.protocol.ColumnAggregationProtocol;
import hbase.sort.ResultMapSort;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class TopNExec {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTableInterface table = new HTable(conf, "cups_1");
		//定义查询器
		final Scan scan = new Scan();
		//制定列名
		QualifierFilter filter = new QualifierFilter(CompareOp.EQUAL,
				new SubstringComparator("settleAt2012"));
		scan.setFilter(filter);
		final int n = 10;
		//定义获取map
		
		Map<byte[], KeyValue[]> map = null;
		long start = System.currentTimeMillis();
		try {
			//获取每个region的topN
			map = table.coprocessorExec(ColumnAggregationProtocol.class,
					scan.getStartRow(), scan.getStopRow(),
					new Batch.Call<ColumnAggregationProtocol, KeyValue[]>() {
						public KeyValue[] call(
								ColumnAggregationProtocol instance)
								throws IOException {
							return instance.topN(scan, n);
						}

					});
		} catch (Throwable e) {
			e.printStackTrace();
		}
		//定义未排序的map
		Map<String,Long> unSortedMap = new HashMap<String, Long>();
		for (Map.Entry<byte[], KeyValue[]> entry : map.entrySet()) {
			KeyValue[] temp = entry.getValue();
			for (int i = 0; i < temp.length; i++) {
				//加入每个region的每个值
				unSortedMap.put(Bytes.toString(temp[i].getRow()), Bytes.toLong(temp[i].getValue()));
			}
		}
		//排序
		List<Map.Entry<String, Long>> list = new LinkedList<Map.Entry<String,Long>>(unSortedMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
			public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
				//降序
				return -o1.getValue().compareTo(o2.getValue());
			}
			
		});
		//输出结果
		int i =1;
		for (Map.Entry<String, Long> entry : list) {
			System.out.println(i++ +": 卡号:" + entry.getKey() +", 交易值:"+entry.getValue()/100.00 +"(元)");
			if (i > n){
				break;
			}
		}
		long end = System.currentTimeMillis();
		System.out.println("time elasped is: " +(end-start) + "(ms).");
		
		table.close();
	}
}
