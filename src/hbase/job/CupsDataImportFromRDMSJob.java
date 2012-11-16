package hbase.job;

import hadoop.jdbc.model.CupsDetail;
import hbase.mapreduce.CupsDataMapper;
import hbase.mapreduce.CupsJSONMapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author liusheding
 * @version 1.0
 * @create_date 2012-8-31
 */
public class CupsDataImportFromRDMSJob {

	public static final String[] fieldNames = { "ID", "PRI_ACCT_NO_CONV",
			"CARD_ATTR", "CUPS_CARD_IN", "ACPT_INS_ID_CD", "FWD_INS_ID_CD",
			"SYS_TRA_NO", "ISS_INS_ID_CD", "TRANS_ID", "TFR_DT_TM",
			"SETTLE_AT", "TERM_ID", "MCHNT_CD", "MCHNT_TP", "REGION_CD",
			"TRANS_CH", "ACPT_TERM_TP", "BIG_MCC", "FEE_TYPE" };

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = HBaseConfiguration.create();
		DBConfiguration.configureDB(conf, "oracle.jdbc.driver.OracleDriver",
				"jdbc:oracle:thin:@192.168.0.120:1521:stressdb", "crm", "crm");
		Job job = new Job(conf, "CupsDataImportFromRDMSJob");
		job.setJarByClass(CupsDataImportFromRDMSJob.class);
		job.setMapperClass(CupsJSONMapper.class);
		DBInputFormat.setInput(job, CupsDetail.class, "CUPS_DETAIL_2", null,
				null, fieldNames);
		TableMapReduceUtil.initTableReducerJob("cups_6", null, job);
		job.setOutputValueClass(Put.class);
		job.setNumReduceTasks(0);
		boolean flag = job.waitForCompletion(true);
		if (!flag) {
			throw new IOException("job执行异常");
		}
	}

	public static String[] getFields() {
		byte[][] qua = CupsDataMapper.qualifier;
		String columns[] = new String[qua.length];
		for (int i = 0; i < qua.length; i++) {
			columns[i] = Bytes.toString(qua[i]);
		}
		return columns;
	}
}
