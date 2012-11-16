package test;

import java.io.IOException;

import hadoop.jdbc.model.CupsDetail;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-9-11
 */
public class JSONTest {
	/**
	 * 
	 * @param args
	 * @throws JSONException 
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonGenerationException 
	 */
	public static void main(String[] args) throws JSONException, JsonGenerationException, JsonMappingException, IOException{
		JSONObject object = new JSONObject("{'a':'232423'}");
		object.put("b", "s");
		JSONObject o1 = new JSONObject("{\"a\":\"232423\",\"b\":\"s\"}");
		System.out.println(object.toString());
		System.out.println(o1.get("a"));
		ObjectMapper o = new ObjectMapper();
		ObjectMapper o2 = new ObjectMapper();
		CupsDetail cd = new CupsDetail();
		o.writeValueAsString(cd);
		String  st = o.writeValueAsString(cd);
		CupsDetail cd1 = o2.readValue(st, CupsDetail.class);
		System.out.println(Bytes.toString(Bytes.toBytes(st)));
		System.out.println(cd1.getSettleAt());
	}
}
