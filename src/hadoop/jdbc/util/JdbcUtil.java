package hadoop.jdbc.util;

/**
 * @author liusheding
 * @version 1.0
 * @create_date 2012-8-30
 */
public class JdbcUtil {
	public static void main(String[] args) {

		byte[] a = "Hello,Long Time No See !".getBytes();

		int i = 0;

		while (i < a.length || false) {

			System.out.println(a[i >= ( a.length-1) ? a.length-1 :i ]);
			
			i++;
		}
	}

}
