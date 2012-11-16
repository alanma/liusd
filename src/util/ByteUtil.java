 package util;

import hadoop.jdbc.model.Cups;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-28
 */
public class ByteUtil {
	static final double[] EVERY_SIXTEENTH_FACTORIAL = {
	      0x1.0p0,
	      0x1.30777758p44,
	      0x1.956ad0aae33a4p117,
	      0x1.ee69a78d72cb6p202,
	      0x1.fe478ee34844ap295,
	      0x1.c619094edabffp394,
	      0x1.3638dd7bd6347p498,
	      0x1.7cac197cfe503p605,
	      0x1.1e5dfc140e1e5p716,
	      0x1.8ce85fadb707ep829,
	      0x1.95d5f3d928edep945};
	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		ObjectMapper o = new ObjectMapper();
		Cups cups = o.readValue("{\"priAcctNoConv\":\"1212345678912341385\",\"tfrDtTm\":\"20120102130126\"," +
				"\"sysTraNo\":\"8432\",\"settleAt\":148967,\"cardAttr\":\"01\",\"cupsCardIn\":\"1\",\"acptInsIdCd\":" +
				"\"34038324420\",\"fwdInsIdCd\":\"34038324420\",\"issInsIdCd\":\"34038324420\",\"transId\":\"s01\"," +
				"\"termId\":\"8432\",\"mchntCd\":" +
				"\"898120083510026\",\"mchntTp\":\"8432\"" +
				",\"regionCd\":\"5543\",\"transCh\":" +
				"\"01\",\"acptTermTp\":\"01\"}", Cups.class);
		System.out.println(cups.getPriAcctNoConv());
		Field field = Cups.class.getDeclaredField("mchntTp");
		System.out.println(field.get(cups));
		Map<Object, Long> map = new HashMap<Object, Long>();
		map.put("1", 100L);
		PrintStream s = System.out;
		s.println("fuck");
		s.println("you");
		increment(map, "1", 152L);
		System.out.println(map.get("1"));
		Class clazz = Cups.class;
		Object a = o.readValue("{\"priAcctNoConv\":\"1212345678912341385\",\"tfrDtTm\":\"20120102130126\"," +
				"\"sysTraNo\":\"8432\",\"settleAt\":148967,\"cardAttr\":\"01\",\"cupsCardIn\":\"1\",\"acptInsIdCd\":" +
				"\"34038324420\",\"fwdInsIdCd\":\"34038324420\",\"issInsIdCd\":\"34038324420\",\"transId\":\"s01\"," +
				"\"termId\":\"8432\",\"mchntCd\":" +
				"\"898120083510026\",\"mchntTp\":\"8432\"" +
				",\"regionCd\":\"5543\",\"transCh\":" +
				"\"01\",\"acptTermTp\":\"01\"}", clazz);
		System.out.println(field.get(a));
	}
	
	protected static void increment(Map<Object, Long> map,Object key,Long value) {
		Long curVal = map.get(key);
		curVal += value;
		map.put(key, curVal);
	}
}
