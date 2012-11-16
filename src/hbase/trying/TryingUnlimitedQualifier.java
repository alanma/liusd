package hbase.trying;

import hadoop.jdbc.model.Cups;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author liusheding
 * @version 1.0
 * @create_date 2012-9-19
 */
public class TryingUnlimitedQualifier {

	private static final String file = "/user/hadoop/cups1/1.txt";

	private static final String tableName = "cups_1";

	public static class UnlimitedQualifierMapper extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {
		// 每次提交的最大数目 TODO 需要考虑到内存
		protected final static int MAX_CAPACITY = 1000;

		private static final String indexTable = "index_merchant_1";

		private static final byte[] family = Bytes.toBytes("data");

		protected List<Put> puts;
		protected Configuration conf;
		protected HTablePool pool;
		protected HTableInterface table;
		protected ObjectMapper object = new ObjectMapper();

		protected void setup(Context context) throws IOException,
				InterruptedException {
			conf = context.getConfiguration();
			pool = new HTablePool(conf, Integer.MAX_VALUE);
			puts = new ArrayList<Put>();
			table = pool.getTable(indexTable);
		}

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException { 
			String[] lines = value.toString().split("\\|");
			Cups cups = new Cups(lines);
			if (cups != null && cups.getPriAcctNoConv() != null) {
				byte[] cupsKey = cups.getPriAcctNoConv().getBytes();
				byte[] qualifier = (cups.getTfrDtTm() + cups.getSysTraNo())
						.getBytes();
				byte[] qualifierSettleAt = ("settleAt"+cups.getTfrDtTm() + cups.getSysTraNo())
						.getBytes();
				Put put = new Put(cupsKey);
				put.add(family, qualifier, object.writeValueAsBytes(cups));
				put.add(family, qualifierSettleAt, Bytes.toBytes(cups.getSettleAt()));
				// TODO 自动统计 table.incrementColumnValue(row, cupsKey, qualifier,
				// amount);
				context.write(new ImmutableBytesWritable(cupsKey), put);
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			table.flushCommits();
			puts.clear();
			table.close();
			pool.close();

		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "CUPS文件数据导入任务");
		job.setJarByClass(TryingUnlimitedQualifier.class);
		FileInputFormat.addInputPath(job, new Path(file));
		job.setMapperClass(UnlimitedQualifierMapper.class);
		TableMapReduceUtil.initTableReducerJob(tableName, null, job);
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
