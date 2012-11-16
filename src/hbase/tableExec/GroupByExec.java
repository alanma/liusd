package hbase.tableExec;

import hbase.protocol.ColumnAggregationProtocol;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class GroupByExec {

	/**
	 * @param args
	 * @throws Throwable 
	 */
	public static void main(String[] args) throws Throwable {
		Configuration conf = HBaseConfiguration.create();
		HTableInterface table = new HTable(conf, "cups_1");
		final Scan scan = new Scan();
		final String column = "mchntCd";
		scan.addFamily(Bytes.toBytes("data"));
		Filter filter = new ColumnPrefixFilter(Bytes.toBytes("2012"));
		scan.setFilter(filter);
		long start = System.currentTimeMillis();
		Map<byte[], Map<Object, Long>> map = table.coprocessorExec(
				ColumnAggregationProtocol.class, scan.getStartRow(),
				scan.getStopRow(),
				new Batch.Call<ColumnAggregationProtocol, Map<Object, Long>>() {
					public Map<Object, Long> call(
							ColumnAggregationProtocol instance)
							throws IOException {
						return instance.groupByCount(scan, column);
					}

				});
		// table.close();
		int i = 0;
		Map<Object, Long> unsortedMap = new HashMap<Object, Long>();
		for (Map.Entry<byte[], Map<Object, Long>> entry : map.entrySet()) {
			//list.addAll(entry.getValue().entrySet());
			if (i++ < 1) {
				unsortedMap.putAll(entry.getValue());
			} else {
				for (Map.Entry<Object, Long> entry1:entry.getValue().entrySet()){
					if (unsortedMap.containsKey(entry1.getKey())) {
						increment(unsortedMap, entry1.getKey(), entry1.getValue());
					} else {
						unsortedMap.put(entry1.getKey(), entry1.getValue());
					}
				}
			}
		}
		List<Map.Entry<Object, Long>> list = new LinkedList<Map.Entry<Object,Long>>(unsortedMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<Object, Long>>() {
			public int compare(Entry<Object, Long> o1, Entry<Object, Long> o2) {
				return -o1.getValue().compareTo(o2.getValue()); //降序
			}
			
		});
		//输出
		i = 0;
		for (Map.Entry<Object, Long> entry:list){
			System.out.println(column+" is:"+entry.getKey()+",count is:"+entry.getValue());
			if (i++ > 100) {
				break;
			}
		}
		long end = System.currentTimeMillis();
		System.out.println("time elasped is :"+(end-start)+" (ms).");
	}
	protected static void increment(Map<Object, Long> map, Object key, Long value) {
		Long curVal = map.get(key);
		curVal += value;
		map.put(key, curVal);
	}
}
