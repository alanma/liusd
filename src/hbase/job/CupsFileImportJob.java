package hbase.job;

import hadoop.jdbc.model.Cups;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
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
 * 
 * CUPS文件导入任务主程序
 * 
 * @author liusheding
 * @version 1.0
 * @create_date 2012-9-13
 */
public class CupsFileImportJob {

	/**
	 * 字段名
	 */

	public static final String[] fieldNames = { "ID", "PRI_ACCT_NO_CONV",
			"CARD_ATTR", "CUPS_CARD_IN", "ACPT_INS_ID_CD", "FWD_INS_ID_CD",
			"SYS_TRA_NO", "ISS_INS_ID_CD", "TRANS_ID", "TFR_DT_TM",
			"SETTLE_AT", "TERM_ID", "MCHNT_CD", "MCHNT_TP", "REGION_CD",
			"TRANS_CH", "ACPT_TERM_TP", "BIG_MCC", "FEE_TYPE" };
	/**
	 * 文件路径
	 */
	private static final String filePath = "/user/hadoop/cups";

	private static final String table = "cups";

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
		job.setJarByClass(CupsFileImportJob.class);
		FileInputFormat.addInputPath(job, new Path(filePath));
		job.setMapperClass(FileMapper.class);
		TableMapReduceUtil.initTableReducerJob(table, null, job);
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class FileMapper extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {

		protected final static byte[] family = Bytes.toBytes("data");

		protected final static byte[] qualifier = Bytes.toBytes("json");

		protected final static byte[] indexFamily = Bytes.toBytes("index");

		protected final static String indexTable = "index_merchant";

		protected final static byte[] indexQualifier1 = Bytes
				.toBytes("cupsKey");

		protected final static byte[] indexQualifier2 = Bytes
				.toBytes("settleAt");
		// 每次提交的最大数目 TODO 需要考虑到内存
		protected final static int MAX_CAPACITY = 1000;

		protected List<Put> puts;
		protected Configuration conf;
		protected HTablePool pool;
		protected HTableInterface table;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			conf = context.getConfiguration();
			pool = new HTablePool(conf, Integer.MAX_VALUE);
			puts = new ArrayList<Put>();
			table = pool.getTable(indexTable);
		}

		@Override
		protected void map(LongWritable key, Text line, Context context)
				throws IOException, InterruptedException {
			String[] value = line.toString().split("\\|");
			
			Cups data = new Cups(value);
			String keys[] = data.showKeys();
			String cupsKeyStr = keys[0];
			String merchantKey = keys[1];
			// key为空时mapper不输出
			if (cupsKeyStr != null) {
				// 1 生成主表的PUT
				ImmutableBytesWritable cupsKey = new ImmutableBytesWritable(
						cupsKeyStr.getBytes());
				Put put = new Put(cupsKey.get());
				// 去掉ID
				ObjectMapper json = new ObjectMapper();
				String jsonValue = json.writeValueAsString(data);
				put.add(family, qualifier, Bytes.toBytes(jsonValue));
				byte[] settleAt = Bytes.toBytes(data.getSettleAt() == null ? 0L
						: data.getSettleAt());
				// 增加金额列方便统计
				put.add(family, indexQualifier2, settleAt);
				// 2 主表写
				context.write(cupsKey, put);
				// 3 生成索引表PUT
				Put indexPut = new Put(Bytes.toBytes(merchantKey));
				indexPut.add(indexFamily, indexQualifier1, cupsKey.get());
				indexPut.add(indexFamily, indexQualifier2, settleAt);
				puts.add(indexPut);
				// 4 写索引表
				if (puts.size() >= MAX_CAPACITY) {
					table.batch(puts);
					table.flushCommits();
					puts.clear();
				}
				// 直接写,由hbase控制批量提交数目
				// table.put(indexPut);
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
}
