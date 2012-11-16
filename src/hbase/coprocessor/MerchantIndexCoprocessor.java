package hbase.coprocessor;

import hadoop.jdbc.model.CupsDetail;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 商户二级索引表coprocessor
 *@author liusheding
 *@version 1.0
 *@create_date 2012-9-5
 */
public class MerchantIndexCoprocessor extends BaseRegionObserver {
	
	private static final byte[] INDEX_MERCHANT_CUPS_TABLE = Bytes.toBytes("index_merchant_cups_4"); 
	private static final byte[] indexfamily = Bytes.toBytes("index");
	private static final byte[] indexqualifier = Bytes.toBytes("cupskey");
	private static final byte[] family = Bytes.toBytes("data");
	private static final byte[] qualifier = Bytes.toBytes("json");
	 /**
	  * 在cups_detail表更新数据后，向索引表插入一条数据
	  */
	 @Override
	 public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e, 
	      final Put put, final WALEdit edit, final boolean writeToWAL) throws IOException {
		 
		HTableInterface indexTable =  e.getEnvironment().getTable(INDEX_MERCHANT_CUPS_TABLE);
		String cupsRow = Bytes.toString(put.getRow());
		String mechant = cupsRow.substring(35,50);
		String card = cupsRow.substring(0,21);
		String time = cupsRow.substring(21,35);
		String sysNo = cupsRow.substring(50);
		String merchantRow = mechant+time+card+sysNo;
		ObjectMapper json = new ObjectMapper();
		CupsDetail cd = json.readValue(Bytes.toString(put.get(family, qualifier).get(0).getValue()), CupsDetail.class);
		if(cd.getSettleAt() == null ){
			cd.setSettleAt(0L);
		}
		Put  put1 = new Put(Bytes.toBytes(merchantRow));
		put1.add(indexfamily,indexqualifier,put.getRow());
		put1.add(indexfamily,indexqualifier,Bytes.toBytes(cd.getSettleAt()));
		indexTable.put(put1);
		indexTable.close();
	  }
	 
	
}
