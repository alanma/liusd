package hbase.mapreduce;

import hadoop.jdbc.model.CardgradeSummaryPO;
import hbase.MyDBOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;



import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-29
 */
public class CardgradeSummary {
	
	private static final Log log = LogFactory.getLog(CardgradeSummary.class);

	public static final String JOB_NAME = "CardgradeSummaryJob";
	
	public static String INPUTTABLE = "card";
	
	public static String OUTPUTTABLE = "cardgrade_1";
	
	public static String OUTPATH = "/usr/hadoop/hightcardbin/summary";
	
	public static byte[] family = Bytes.toBytes("data");
	
	public static byte[] qualifier = Bytes.toBytes("cardgrade");
	
	public enum Type {hbaseTable,hadoopFile,rdmsTable};
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, SQLException {
		Configuration conf = HBaseConfiguration.create();
		Scan scan  = new Scan();
		scan.addColumn(family, qualifier);
		Job job = createJob(conf, scan, Type.hbaseTable); 
		//Job job = createJob(conf,scan,Type.hadoopFile);
		//Job job = createJob(conf,scan,Type.rdmsTable);
		boolean flag = true;
		flag=job.waitForCompletion(true);
		if(!flag){
			throw new IOException("job 执行异常");
		}
	}
	
	public static Job createJob(Configuration conf,Scan scan,Type type) throws IOException, ClassNotFoundException, SQLException{
		Job job = new Job(conf, JOB_NAME+"to:"+type);
		job.setJarByClass(CardgradeSummary.class);
		switch (type) {
		case hbaseTable:
			TableMapReduceUtil.initTableMapperJob(INPUTTABLE, scan, CardgradeMapper.class, ImmutableBytesWritable.class, IntWritable.class, job);
			TableMapReduceUtil.initTableReducerJob(OUTPUTTABLE, HBaseTableReducer.class, job);
			break;
		case hadoopFile:
			TableMapReduceUtil.initTableMapperJob(INPUTTABLE, scan, CardgradeMapper.class,ImmutableBytesWritable.class , IntWritable.class, job);
			job.setReducerClass(HadoopFileReducer.class);
			FileOutputFormat.setOutputPath(job,new Path(OUTPATH));
			break;
		case rdmsTable:
			DBConfiguration.configureDB(conf, "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@192.168.0.200:1521:crmproduct", "hive", "hive");
			System.out.println(conf.get("mapreduce.jdbc.driver.class"));
			System.out.println(conf.get("mapreduce.jdbc.url"));
			Job job1 = new Job(conf, JOB_NAME+"to:"+type);
			TableMapReduceUtil.initTableMapperJob(INPUTTABLE, scan, CardgradeMapper.class,ImmutableBytesWritable.class , IntWritable.class, job1);
			String[] filedNames = {"grade","num"};
			DBOutputFormat.setOutput(job1, "cardgradesummary",filedNames);
			job1.setReducerClass(RDMSTableReucer.class);
			job1.setOutputFormatClass(MyDBOutputFormat.class);
			System.out.println(job1.getConfiguration().get("mapreduce.jdbc.driver.class"));
			return job1;
			/**
			 *  测试DB连接代码
				DBConfiguration dbConf = new DBConfiguration(conf);
				Connection connection = dbConf.getConnection();
				PreparedStatement statement = connection.prepareStatement("select 1 from dual");
				ResultSet res = statement.executeQuery();
				while(res.next()){
					System.out.println(res.getObject(1));
				}
				statement.close();
				connection.close();
			*/
		default:
			break;
		}
		return job;
	}
	/**
	 * map 统计Htable里面的源数据
	 * @author liusd
	 *
	 */
	static class CardgradeMapper 
		extends TableMapper<ImmutableBytesWritable, IntWritable>{
		
		private static IntWritable value = new IntWritable(1);
		
		public void map(ImmutableBytesWritable key,Result result,Context context ){
			
			try {
				byte[] grade = result.getValue(family, qualifier);
				if(grade==null){
					grade = "null".getBytes();
				}
				ImmutableBytesWritable cardgrade = 
					new ImmutableBytesWritable(grade);
				
				context.write(cardgrade, value);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		public static void main(String[] args){
			System.out.println("s");
		}
		
	}
	/**
	 * reduce将统计的数据到另外一个hbase表里面去
	 * @author liusd
	 *
	 */
	static class HBaseTableReducer 
		extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable>{
		
		private final static byte[] afam = "summary".getBytes();
		
		private final static byte[] col  = "num".getBytes(); 
		
		public void reduce(ImmutableBytesWritable cardgrade,Iterable<IntWritable> value,Context context){
			try {
				Put put = new Put(cardgrade.get());
				long sum = 0L ;
				for(IntWritable i : value){
					sum +=i.get();
				}
				put.add(afam, col,Bytes.toBytes(sum));
				context.write(cardgrade, put);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	/**
	 * reduce 将统计的数据存放到hdfs
	 * @author liusd
	 *
	 */
	static class HadoopFileReducer
		extends Reducer<ImmutableBytesWritable, IntWritable, Text, IntWritable>{
		
		IntWritable count =  new IntWritable();
		
		public void reduce(ImmutableBytesWritable cardgrade,Iterable<IntWritable> value,Context context){
			
			try {
				Text text = new Text(cardgrade.get());
				int sum = 0 ;
				for(IntWritable i : value){
					sum += i.get();
				}
				count.set(sum);
				context.write(text, count);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	/**
	 * 将统计的数据存放到关系型数据库表里面
	 * @author liusd
	 *
	 */
	static class RDMSTableReucer
		extends Reducer<ImmutableBytesWritable, IntWritable, CardgradeSummaryPO,Text>{
		DBConfiguration dbConf = null;
		public void setup(Context context){
			dbConf = new DBConfiguration(context.getConfiguration());
			System.out.println(dbConf.getOutputTableName()+dbConf.getOutputFieldNames()+dbConf.getOutputFieldCount());
			
		}
		
		public void reduce(ImmutableBytesWritable cardgrade,Iterable<IntWritable> value,Context context){
			try {
				Text text = new Text(cardgrade.get());
				int sum = 0 ;
				for(IntWritable i : value){
					sum += i.get();
				}
				CardgradeSummaryPO po = new CardgradeSummaryPO(text.toString(), sum);
				System.out.println(po.toString());
				context.write(po, text);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
