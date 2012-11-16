package hbase.protocol;


import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
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
public interface ColumnAggregationProtocol extends CoprocessorProtocol {
	/**
	 * 求出某列或者某列前缀的值的和
	 * @param scan
	 * @return
	 * @throws IOException
	 */
	public Long sum(final Scan scan) throws IOException;
	/**
	 * 求出TopN
	 * @param scan
	 * @param i
	 * @return
	 * @throws IOException
	 */
	public KeyValue[] topN(final Scan scan,final int i) throws IOException;
	/**
	 * 分组统计count
	 * @param scan
	 * @param prexColumn
	 * @return
	 * @throws IOException
	 */
	public Map<Object, Long> groupByCount(final Scan scan,final String column) throws IOException;
	/**
	 * 分组统计sum
	 * @param scan
	 * @param column
	 * @return
	 * @throws IOException
	 */
	public Map<Object, Long> groupBySum(final Scan scan,final String column) throws IOException;
	
	/**
	 * 分组平均统计 
	 * @param scan
	 * @param prexColumn
	 * @return
	 * @throws Exception
	 */
	public Map<Object, AvgPair> groupByAvg(final Scan scan,final String prexColumn) throws IOException;
}
