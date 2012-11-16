package test;


public class HelloHadoop {
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		
		if ( args[0] == null ^ args[1] == null ){
			System.out.println("either one false");
		} else if ( args[0] == null ) {
			System.out.println("both false");
		} else {
			System.out.println("both true");
		}
		int i = 0x10;
		i <<= 0x4;
		System.out.println(i);
	}
	
}
