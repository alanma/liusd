package hbase.protocol;

import hadoop.jdbc.model.Cups;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;


import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * <p>描述:  </p>
 *
 * <p>原则:   </p>
 *
 * <p>作用:   </p>
 *
 * <p>Create Time:  2012-10-24 <p>
 *
 * @author liusheding
 * @version 1.1
 */

public class ColumnAggegationImpl extends BaseEndpointCoprocessor implements
		ColumnAggregationProtocol {

	private static final Log log = LogFactory
			.getLog(ColumnAggegationImpl.class);

	public Long sum(final Scan scan) throws IOException {
		Long sumResult = 0L;
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);
		try {
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			do {
				done = scanner.next(curVals);
				for (KeyValue kv : curVals) {
					Long temp = Bytes.toLong(kv.getValue());
					sumResult += temp;
				}
				curVals.clear();
			} while (done);
			log.info(((RegionCoprocessorEnvironment) getEnvironment())
					.getRegion().getRegionNameAsString()
					+ "sum value is :"
					+ sumResult);
		} finally {
			scanner.close();
		}
		return sumResult;
	}

	public KeyValue[] topN(final Scan scan, final int n) throws IOException {
		// 求出每个region的top N

		KeyValue[] topN = new KeyValue[n];
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);
		try {
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			do {
				done = scanner.next(curVals);
				for (KeyValue kv : curVals) {
					int i = getMin(topN);
					if (compare(kv, topN[i]) > 0) {
						topN[i] = kv;
					}
				}
				curVals.clear();
			} while (done);
			log.info("region: "+ ((RegionCoprocessorEnvironment) getEnvironment())
							.getRegion().getRegionNameAsString()
					+ " topN operation is done!");
		} finally {
			scanner.close();
		}
		return topN;
	}

	/**
	 * 获取最小的值
	 * 
	 * @param topN
	 * @return
	 */
	protected int getMin(KeyValue[] topN) {
		//填充空
		for (int j = 0; j < topN.length; j++) {
			if (topN[j] == null || topN[j].getValue() == null) {
				return j;
			}
		}
		int i = 0;
		KeyValue temp = topN[i];
		for (int j = 1; j < topN.length; j++) {
			if (Bytes.toLong(topN[j].getValue()) < Bytes
					.toLong(temp.getValue())) {
				i = j;
				temp = topN[i];
			}
		}
		return i;
	}
	/**
	 * 获取最大值
	 * @param topN
	 * @return
	 */
	protected int getMax(KeyValue[] topN) {
		//填充空
		for (int j = 0; j < topN.length; j++) {
			if (topN[j] == null || topN[j].getValue() == null) {
				return j;
			}
		}
		int i = 0;
		KeyValue temp = topN[i];
		for (int j = 1; j < topN.length; j++) {
			if (Bytes.toLong(topN[j].getValue()) > Bytes
					.toLong(temp.getValue())) {
				i = j;
				temp = topN[i];
			}
		}
		return i;
	}

	/**
	 * 比较两个值
	 * 
	 * @param kv1
	 * @param kv2
	 * @return
	 */
	public int compare(KeyValue kv1, KeyValue kv2) {
		if (kv2 == null && kv1 == null) {
			return 0;
		}
		if (kv2 == null) {
			return 1;
		}
		if (kv1 == null) {
			return -1;
		}
		Long val1 = Bytes.toLong(kv1.getValue());
		Long val2 = Bytes.toLong(kv2.getValue());
		return val1.compareTo(val2);
	}

	public Map<Object, Long> groupByCount(Scan scan, String prexColumn)
			throws IOException {
		Map<Object, Long> map = new HashMap<Object, Long>();
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);
		ObjectMapper om = new ObjectMapper();
		try {
			Field col = Cups.class.getDeclaredField(prexColumn);
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			long one = 1L;
			do {
				done = scanner.next(curVals);
				for (KeyValue kv : curVals) {
					Cups cups = om.readValue(Bytes.toString(kv.getValue()),
							Cups.class);
					String colVal = (String) col.get(cups);
					if (map.containsKey(colVal)) {
						increment(map, colVal, one);
					} else {
						map.put(colVal, one);
					}

				}
				curVals.clear();
			} while (done);
			log.info("the c "+ prexColumn+ " group by count operation in region "
					+ ((RegionCoprocessorEnvironment) getEnvironment()).getRegion().getRegionNameAsString() + " is done ,map size is :" + map.size());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			scanner.close();
		}
		return map;
	}

	/**
	 * 增加map指定KEY的值
	 * 
	 * @param map
	 * @param key
	 * @param value
	 */
	protected void increment(Map<Object, Long> map, Object key, Long value) {
		Long curVal = map.get(key);
		curVal += value;
		map.put(key, curVal);
	}

	public Map<Object, AvgPair> groupByAvg(Scan scan, String prefixColumn)
			throws IOException {
		Map<Object, AvgPair> map = new HashMap<Object, AvgPair>();
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);
		ObjectMapper om = new ObjectMapper();
		try {
			Field col = Cups.class.getDeclaredField(prefixColumn);
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			do {
				done = scanner.next(curVals);
				for (KeyValue kv : curVals) {
					Cups cups = om.readValue(Bytes.toString(kv.getValue()),
							Cups.class);
					String colVal = (String) col.get(cups);
					Long agrVal = cups.getSettleAt();
					if (map.containsKey(colVal)) {
						map.get(colVal).update(agrVal);
					} else {
						map.put(colVal, new AvgPair(agrVal));
					}
				}
				curVals.clear();
			} while (done);
			log.info("the column"+prefixColumn+"group by avg operation in "+((RegionCoprocessorEnvironment) getEnvironment())
					.getRegion().getRegionNameAsString()+" is done,map size is"+map.size());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			scanner.close();
		}
		return map;
	}

	public Map<Object, Long> groupBySum(Scan scan, String column)
			throws IOException {
		Map<Object, Long> map = new HashMap<Object, Long>();
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);
		ObjectMapper om = new ObjectMapper();
		try {
			Field field = Cups.class.getDeclaredField(column);
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			do {
				done = scanner.next(curVals);
				for (KeyValue kv : curVals) {
					Cups cups = om.readValue(Bytes.toString(kv.getValue()), Cups.class);
					String colVal = (String) field.get(cups);
					Long settleAt = cups.getSettleAt();
					if (map.containsKey(colVal)) {
						increment(map, colVal, settleAt);
					} else {
						map.put(colVal, settleAt);
					}
				}
			} while (done);
			log.info("the column"+column+"group by sum operation in "+((RegionCoprocessorEnvironment) getEnvironment())
					.getRegion().getRegionNameAsString()+" is done,map size is"+map.size());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			scanner.close();
		}
		return map;
	}
	
}
