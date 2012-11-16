package hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import hadoop.jdbc.model.CupsDetail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * cups数据生产JSON格式数据 mapper 类
 * 
 * @author liusheding
 * @version 1.0
 * @create_date 2012-9-11
 */
public class CupsJSONMapper extends
		Mapper<LongWritable, CupsDetail, ImmutableBytesWritable, Writable> {

	protected final static byte[] family = Bytes.toBytes("data");

	protected final static byte[] qualifier = Bytes.toBytes("json");

	protected final static byte[] indexFamily = Bytes.toBytes("index");

	protected final static byte[] indexQualifier1 = Bytes.toBytes("cupsKey");

	protected final static byte[] indexQualifier2 = Bytes.toBytes("settleAt");
	//每次提交的最大数目 TODO 需要考虑到内存 
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
		table = pool.getTable("index_merchant_cups_6");
	}
	
	protected void map(LongWritable key, CupsDetail value, Context context)
			throws IOException, InterruptedException {
		String keys[] = value.showKeys();
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
			String jsonValue = json.writeValueAsString(value);
			put.add(family, qualifier, Bytes.toBytes(jsonValue));
			byte[] settleAt = Bytes.toBytes(value.getSettleAt() == null ? 0L : value.getSettleAt());
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
