package hbase.mapreduce;

import java.io.IOException;

import hadoop.jdbc.model.CupsDetail;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author liusheding
 * @version 1.0
 * @create_date 2012-8-31
 */
public class CupsDataMapper extends
		Mapper<LongWritable, CupsDetail, ImmutableBytesWritable, Writable> {
	protected final static byte[] family = Bytes.toBytes("data");
	public final static byte[][] qualifier = { Bytes.toBytes("id"),
			Bytes.toBytes("pri_acct_no_conv"), Bytes.toBytes("card_attr"),
			Bytes.toBytes("cups_card_in"), Bytes.toBytes("acpt_ins_id_cd"),
			Bytes.toBytes("fwd_ins_id_cd"), Bytes.toBytes("sys_tra_no"),
			Bytes.toBytes("iss_ins_id_cd"), Bytes.toBytes("trans_id"),
			Bytes.toBytes("tfr_dt_tm"), Bytes.toBytes("settle_at"),
			Bytes.toBytes("term_id"), Bytes.toBytes("mchnt_cd"),
			Bytes.toBytes("mchnt_tp"), Bytes.toBytes("region_cd"),
			Bytes.toBytes("trans_ch"), Bytes.toBytes("acpt_term_tp"),
			Bytes.toBytes("big_mcc"), Bytes.toBytes("fee_type") };

	protected void map(LongWritable key, CupsDetail value, Context context)
			throws IOException, InterruptedException {
		String cupsKeyStr = value.showKeys()[0];
		// key为空时mapper不输出
		if (cupsKeyStr != null) {
			ImmutableBytesWritable cupsKey = new ImmutableBytesWritable(
					cupsKeyStr.getBytes());
			Put put = new Put(cupsKey.get());
			for (int i = 0; i < qualifier.length; i++) {
				if (i == 0 || i == 10) {
					if (value.getValue(i) == null) {
						put.add(family, qualifier[i], Bytes.toBytes((long) 0));
					} else {
						put.add(family, qualifier[i],
								Bytes.toBytes((Long) value.getValue(i)));
					}
				} else {
					if (value.getValue(i) == null) {
						put.add(family, qualifier[i], Bytes.toBytes(""));
					} else {
						put.add(family, qualifier[i],
								Bytes.toBytes((String) value.getValue(i)));
					}
				}
			}
			context.write(cupsKey, put);
		}
	}
}
