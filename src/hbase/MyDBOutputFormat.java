package hbase;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat.DBRecordWriter;

/**
 *@author liusheding
 *@version 1.0
 *@param <V>
 *@create_date 2012-8-30
 */
public class MyDBOutputFormat<K extends DBWritable, V> extends DBOutputFormat<K , V> {
	  /**
	   * 修改源码的异常打印代码，源码不方便看到跟踪异常信息
	   */
	  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) 
		      throws IOException {
		    DBConfiguration dbConf = new DBConfiguration(context.getConfiguration());
		    String tableName = dbConf.getOutputTableName();
		    String[] fieldNames = dbConf.getOutputFieldNames();
		    
		    if(fieldNames == null) {
		      fieldNames = new String[dbConf.getOutputFieldCount()];
		    }
		    
		    try {
		      Connection connection = dbConf.getConnection();
		      PreparedStatement statement = null;
		  
		      statement = connection.prepareStatement(
		                    constructQuery(tableName, fieldNames));
		      return new DBRecordWriter(connection, statement);
		    } catch (Exception ex) {
		    	ex.printStackTrace();
		      throw new IOException(ex.getMessage());
		    }
		  }
	  /**	   
	   * 去掉 hadoop 源码的拼接SQL末尾的';'.该符号会产生ORA-00911：无效字符异常
	   */
	  public String constructQuery(String table, String[] fieldNames) {
		    if(fieldNames == null) {
		      throw new IllegalArgumentException("Field names may not be null");
		    }

		    StringBuilder query = new StringBuilder();
		    query.append("INSERT INTO ").append(table);

		    if (fieldNames.length > 0 && fieldNames[0] != null) {
		      query.append(" (");
		      for (int i = 0; i < fieldNames.length; i++) {
		        query.append(fieldNames[i]);
		        if (i != fieldNames.length - 1) {
		          query.append(",");
		        }
		      }
		      query.append(")");
		    }
		    query.append(" VALUES (");

		    for (int i = 0; i < fieldNames.length; i++) {
		      query.append("?");
		      if(i != fieldNames.length - 1) {
		        query.append(",");
		      }
		    }
		    query.append(")");

		    return query.toString();
		  }
}
