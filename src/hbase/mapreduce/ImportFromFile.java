package hbase.mapreduce;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;



/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-27
 */
public class ImportFromFile {
	
	private static final Log log = LogFactory.getLog(ImportFromFile.class);

	public static final String JOB_NAME = "ImportFileJob";
	
	public static String TABLE = "card";
	
	public static String INPUT = "/user/hadoop/hightcardbin/";
	
	public enum Counters { LINES };
	
	static class ImportMapper 
	extends Mapper<LongWritable, Text, ImmutableBytesWritable,Writable>{
		
		private byte[] family  = null;
		private byte[][] qualifier = null;
		@Override
		protected void setup(Context context ) throws IOException {
			String column = context.getConfiguration().get("conf.column.cell");
			System.out.println(column);
			String columns[] = column.split(":");
			family = Bytes.toBytes(columns[0]);
			qualifier = new byte[columns.length-1][];
			if(columns.length>1){
				for(int i=1;i<columns.length;i++){
					qualifier[i-1] = Bytes.toBytes(columns[i]);
					System.out.println(columns[i]);
				}
			}else{
				qualifier[0] = Bytes.toBytes("qual0");
			}
		}
		
		@Override
		public void map(LongWritable offset,Text line,Context context)
			throws IOException{
			try{
				String data[] = line.toString().split("\\	 {0,9}");
				System.out.println("数据长度："+data.length+", 列长："+qualifier.length);
				if(data.length != qualifier.length){
					Put put = new Put("Exception".getBytes());
					put.add("data".getBytes(), "E".getBytes(), "e".getBytes());
					put.add("data".getBytes(), "F".getBytes(), "f".getBytes());
					context.write(new ImmutableBytesWritable("Exception".getBytes()), put);
					//throw new ImportDataException("数据不正确.Put put = new Put(rowkey);");
				}else{
				//System.out.println(data);
				byte[] rowkey = Bytes.toBytes(data[0]);
				Put put = new Put(rowkey);
				for(int i = 0;i<data.length;i++){
					KeyValue kv = new KeyValue(rowkey, family, qualifier[i],Bytes.toBytes(data[i]));
					//put.add(family, qualifier[i], Bytes.toBytes(data[i]));
					put.add(kv);
				}
				context.write(new ImmutableBytesWritable(rowkey), put);
				log.info(put.toJSON());
				context.getCounter(Counters.LINES).increment(1);
				}
			}catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		
	}

	static class ImportReducer extends TableReducer<ImmutableBytesWritable, Put, ImmutableBytesWritable>{
		
		@Override
		public void reduce(ImmutableBytesWritable key,Iterable<Put> value,Context context){
			try {
				context.write(key, value.iterator().next());
				log.info(key.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		Configuration conf = HBaseConfiguration.create();
		log.info(conf.toString());
		System.out.println(conf.get("hbase.zookeeper.quorum"));
		System.out.println(conf.get("importtsv.bulk.output"));
		conf.set("conf.column.cell",
		"data:id_bin:binno:issbin:cardtype:changedigits:startdigits:startvalue:endvalue:cardgrade");
		Job job = new Job(conf,JOB_NAME+"import from file :"+INPUT+" to Htable "+TABLE);
		job.setJarByClass(ImportFromFile.class);
		job.setMapperClass(ImportMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TableOutputFormat.class);
		//job.setOutputKeyClass(ImmutableBytesWritable.class);
		//job.setOutputValueClass(Put.class);
		//job.setNumReduceTasks(2);
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setReducerClass(PutSortReducer.class);
		FileInputFormat.addInputPath(job, new Path(INPUT));
		TableMapReduceUtil.initTableReducerJob(TABLE, null, job);
	    job.setNumReduceTasks(0);
	    TableMapReduceUtil.addDependencyJars(job);

		System.exit(job.waitForCompletion(true)?0:1);
	}

}
