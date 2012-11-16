package test;
/**
 *@author liusheding
 *@version 1.0
 *@create_date 2012-8-21
 */
public class Code {
	
	public Code(){
		System.out.println("Code instance is runing : "+
		this.getClass().getPackage());
	}
	
	public static void main(String[] args){
		Code code = new Code();
		code.id = 1;
		byte[] a = code.getDebug().getBytes();
		a.toString();
	}
	
	private int id;
	private String debug;
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getDebug() {
		return debug;
	}

	public void setDebug(String debug) {
		this.debug = debug;
	}
	
}