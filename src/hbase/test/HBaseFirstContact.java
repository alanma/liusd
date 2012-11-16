package hbase.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-22
 */
public class HBaseFirstContact {
	
	public static Configuration conf = HBaseConfiguration.create();
	
	public static void createTable(String tableName,String colFamily) throws IOException{
		HBaseAdmin admin = new HBaseAdmin(conf);
		if(!admin.tableExists(Bytes.toBytes(tableName))){
			HTableDescriptor desc = new HTableDescriptor(tableName);
			HColumnDescriptor column = new HColumnDescriptor(colFamily);
			//column.setBloomFilterType(BloomType.ROW);
			//column.setMaxVersions(1);
			desc.addFamily(column);
			//desc.addCoprocessor("hbase.coprocessor.RegionCoprocessorTest",new Path("hdfs://master.hadoop:9000/user/hbase/mycoprocessor.jar"), 100, null);
			//liusd//desc.addCoprocessor(className, jarFilePath, priority, kvs);
			admin.createTable(desc);
			///HTable table ;
			//desc.
		}else{
			throw new TableExistsException(tableName+"表已经存在!");
		}
	}
	public static void main(String[] args){
		try {
			createTable("summary_time_1","data");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}		
	public static void addFamily(String tableName,String colFamily) throws IOException{
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor desc = new HTableDescriptor(tableName);
		admin.disableTable(tableName);
		desc.addFamily(new HColumnDescriptor(colFamily));
		admin.enableTable(tableName);
	}
}
