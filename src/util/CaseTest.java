package util;
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
public class CaseTest{
	enum Type {
		color(10,29,10),
		width(1,1,1),
		height(0,0,0);
		private Type(int a ,int b ,int c) {
			redValue = a;
			greenValue = b;
			blueValue = c;
		}
		private int redValue;
		private int greenValue;
		private int blueValue;
		
		public String toString(){
			return super.toString()+"("+redValue+".........)";
		}
		public int getRedValue() {
			return redValue;
		}
		public void setRedValue(int redValue) {
			this.redValue = redValue;
		}
		public int getGreenValue() {
			return greenValue;
		}
		public void setGreenValue(int greenValue) {
			this.greenValue = greenValue;
		}
		public int getBlueValue() {
			return blueValue;
		}
		public void setBlueValue(int blueValue) {
			this.blueValue = blueValue;
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		CaseTest.Type a = Type.color;
		Type c =  Enum.valueOf(Type.class, "color");
		System.out.println(c);
		for (Type b :Type.values()){
			System.out.println(b);
		}
	}
}
