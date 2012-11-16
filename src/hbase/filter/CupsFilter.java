package hbase.filter;

import hadoop.jdbc.model.Cups;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * <p>描述:   </p>
 *
 * <p>原则:   </p>
 *
 * <p>作用:  用来完成cups value的 过滤器</p>
 *
 * <p>Create Time:  2012-11-1 <p>
 *
 * @author liusheding
 * @version 1.1
 */
public class CupsFilter extends FilterBase implements Filter {
	
	public static final Log log = LogFactory.getLog(CupsFilter.class);
	//filter coloumn
	private Field[] columns;
	//String columns  to Serialize and Deserialize
	private String[] stringColumns;
	//qulifier
	private String qualifier;
	//json mapper
	private  final  ObjectMapper om = new ObjectMapper(); 
	//op
	private CompareOp[] op;
	//compa
	private Object[] value;
	//and or
	private FilterList.Operator andOr = Operator.MUST_PASS_ALL;
	
	public CupsFilter() {
		//do not use it
	}
	/**
	 * constructor for CupsFilter
	 * @param qualifier hbase qualifer or its prifex
	 * @param columns cups object filed name ,using reflect
	 * @param op compare Op
	 * @param value the value to compare
	 * @throws SecurityException 
	 * @throws NoSuchFieldException
	 * @throws CustomerFilterException 
	 */
	public CupsFilter(String qualifier,String[] columns,CompareOp[] op,Object[] value) throws SecurityException, NoSuchFieldException, CustomerFilterException{
		if (qualifier == null || columns == null || op == null || value == null ){
			throw new CustomerFilterException("some params is null,can not construct filter...");
		}
		if (columns.length != op.length || columns.length != value.length) {
			throw new CustomerFilterException("param arrays size is not the same,can not constuct filter...");
		}
		this.qualifier = qualifier;
		this.op = op;
		this.value = value;
		this.stringColumns = columns;
		this.columns = new Field[columns.length];
		for (int i = 0;i < columns.length;i++) {
			this.columns[i] = Cups.class.getDeclaredField(columns[i]);
		}
	}
	/**
	 * constructor for CupsFilter
	 * @param qualifier hbase qualifer or its prifex
	 * @param columns cups object filed name ,using reflect
	 * @param op compare Op
	 * @param value the value to compare
	 * @param andOr must_pass_all or must_pass_one 
	 * @throws SecurityException
	 * @throws CustomerFilterException
	 * @throws NoSuchFieldException
	 */
	public CupsFilter(String qualifier,String[] columns,CompareOp[] op,Object[] value,FilterList.Operator andOr) throws SecurityException, CustomerFilterException, NoSuchFieldException{
		this(qualifier,columns,op,value);
		if (andOr == null) {
			throw new CustomerFilterException("operator is null , can not construct filter...");
		}
		this.andOr = andOr;
	}
	public ReturnCode filterKeyValue(KeyValue kv) {
		String tempQualifier = Bytes.toString(kv.getBuffer(),kv.getQualifierOffset(),kv.getQualifierLength());
		if (tempQualifier == null ){
			return ReturnCode.NEXT_ROW;
		}
		if (!tempQualifier.startsWith(qualifier)) {
			return ReturnCode.NEXT_COL;
		} else {
			try {
				Cups cups = om.readValue(Bytes.toString(kv.getBuffer(),kv.getValueOffset(),kv.getValueLength()),Cups.class);
				if (filterCupsValue(cups)) {
					return ReturnCode.INCLUDE;
				} else {
					return ReturnCode.NEXT_COL;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return ReturnCode.NEXT_ROW;
	}
	/**
	 * judge the values
	 * @param cups
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws CustomerFilterException
	 */
	protected boolean filterCupsValue(Cups cups)
			throws IllegalArgumentException, IllegalAccessException,
			CustomerFilterException {
		boolean and = true;
		boolean or = false;
		for (int i = 0; i < columns.length; i++) {
			columns[i].getType();
			Object temp = columns[i].get(cups);
			switch (op[i]) {
			case EQUAL:
				if (compare(value[i], temp) == 0) {
					or = true;
				} else {
					and = false;
				}
				break;
			case NOT_EQUAL:
				if (compare(value[i], temp) != 0) {
					or = true;
				} else {
					and = false;
				}
				break;
			case LESS:
				if (compare(value[i], temp) < 0) {
					or = true;
				} else {
					and = false;
				}
				break;
			case LESS_OR_EQUAL:
				if (compare(value[i], temp) <= 0) {
					or = true;
				} else {
					and = false;
				}
				break;
			case GREATER:
				if (compare(value[i], temp) > 0) {
					or = true;
				} else {
					and = false;
				}
				break;
			case GREATER_OR_EQUAL:
				if (compare(value[i], temp) >= 0) {
					or = true;
				} else {
					and = false;
				}
				break;
			default:
				break;
			}
		}
		if (andOr == Operator.MUST_PASS_ALL) {
			return and;
		} else {
			return or;
		}
	}
	/**
	 * compare the same type of object(just String or Long,if others throw Exception) 
	 * @param a
	 * @param b
	 * @return
	 * @throws CustomerFilterException
	 */
	protected int compare(Object a,Object b) throws CustomerFilterException {
		if (a instanceof String && b instanceof String) {
			return ((String) a ).compareTo((String) b);
		} else if (a instanceof Long && b instanceof Long) {
			return ((Long) a).compareTo((Long) b);
		} else {
			throw new CustomerFilterException("Object type is different,can not compare two values ...");
		}
	}
	//序列化
	public void write(DataOutput out) throws IOException {
		HbaseObjectWritable.writeObject(out, this.stringColumns, String[].class, null);
		Text.writeString(out, this.qualifier);
		HbaseObjectWritable.writeObject(out, this.op, CompareOp[].class,null);
		HbaseObjectWritable.writeObject(out, this.value,Object[].class, null);
		HbaseObjectWritable.writeObject(out, this.andOr, FilterList.Operator.class, null);
	}
	//反序列化
	public void readFields(DataInput in) throws IOException {
		this.stringColumns = (String[]) HbaseObjectWritable.readObject(in, null);
		this.qualifier=Text.readString(in);
		this.op = (CompareOp[]) HbaseObjectWritable.readObject(in, null);
		this.value = (Object[]) HbaseObjectWritable.readObject(in, null);
		this.andOr = (FilterList.Operator) HbaseObjectWritable.readObject(in, null);
		this.columns = new Field[this.stringColumns.length];
		for (int i = 0;i < this.columns.length;i++) {
			try {
				this.columns[i] = Cups.class.getDeclaredField(this.stringColumns[i]);
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (NoSuchFieldException e) {
				e.printStackTrace();
			}
		}
	}
	
	public Field[] getColumns() {
		return columns;
	}
	public String getQualifier() {
		return qualifier;
	}
	public ObjectMapper getOm() {
		return om;
	}
	public CompareOp[] getOp() {
		return op;
	}
	public Object[] getValue() {
		return value;
	}
	public FilterList.Operator getAndOr() {
		return andOr;
	}
	public void setAndOr(FilterList.Operator andOr) {
		this.andOr = andOr;
	}
	public void setColumns(Field[] columns) {
		this.columns = columns;
	}
	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}
	public void setOp(CompareOp[] op) {
		this.op = op;
	}
	public void setValue(Object[] value) {
		this.value = value;
	}
	public String[] getStringColumns() {
		return stringColumns;
	}
	public void setStringColumns(String[] stringColumns) {
		this.stringColumns = stringColumns;
	}
	
}
