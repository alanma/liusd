package hbase.tableExec;

import hbase.protocol.ColumnAggregationProtocol;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;

import org.apache.hadoop.hbase.util.Bytes;

public class TableExec {

	/**
	 * @param args
	 * @throws Throwable
	 */
	public static void main(String[] args) throws Throwable {
		Configuration conf = HBaseConfiguration.create();
		HTableInterface table = new HTable(conf, "cups_1");
		long a = 9223372036854775807L;
		class SumCallBack implements Batch.Callback<Long> {
			private Long result = 0L;

			public Long getResult() {
				return this.result;
			}

			public void update(byte[] region, byte[] row, Long result) {
				System.out.println(result);
				System.out.println(Bytes.toString(row));
				this.result += result;
			}

		}
		final Scan scan = new Scan();
		QualifierFilter filter = new QualifierFilter(CompareOp.EQUAL, new SubstringComparator("settleAt2012"));
		scan.setFilter(filter);
		SumCallBack sumCallBack = new SumCallBack();
		table.coprocessorExec(ColumnAggregationProtocol.class,
				scan.getStartRow(), scan.getStopRow(),
				new Batch.Call<ColumnAggregationProtocol, Long>() {
					public Long call(ColumnAggregationProtocol instance)
							throws IOException {
						return instance.sum(scan);
					}
				}, sumCallBack);
		System.out.println(sumCallBack.getResult());
	}

}
