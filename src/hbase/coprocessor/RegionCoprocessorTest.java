package hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-9-10
 */
public class RegionCoprocessorTest extends BaseRegionObserver {
	
	private static byte[] indexTable = Bytes.toBytes("index_liusd");
	
	private static byte[] indexfamily = Bytes.toBytes("idx");
	
	private static byte[] indexqualifier = Bytes.toBytes("key");
	
	private static byte[] family = Bytes.toBytes("data");
	
	private static byte[] qualifier = Bytes.toBytes("name");
	
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e,
			Put put, WALEdit edit, boolean writeToWAL) throws IOException {
		HTableInterface table =  e.getEnvironment().getTable(indexTable);
		byte[] key = put.getRow();
		byte[] name = put.get(family,qualifier).get(0).getValue();
		Put put1 = new Put(name);
		put1.add(indexfamily,indexqualifier, key);
		table.put(put1);
	}
}
