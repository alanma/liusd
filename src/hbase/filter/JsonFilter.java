package hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * <p>描述:  </p>
 *
 * <p>原则:   </p>
 *
 * <p>作用:   </p>
 *
 * <p>Create Time:  2012-11-2 <p>
 *
 * @author liusheding
 * @version 1.1
 */
public class JsonFilter extends FilterBase implements Filter {
	
	private Class<?> clazz;
	
	private ObjectMapper om = new ObjectMapper();

	public void write(DataOutput out) throws IOException {
		clazz.getClassLoader();
		 Object d = om.readValue("a", clazz);
	}
	public void readFields(DataInput in) throws IOException {
		
	}
	public Class getClazz() {
		return clazz;
	}
	public ObjectMapper getOm() {
		return om;
	}
	
}
