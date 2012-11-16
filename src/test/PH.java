package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

/**
 * @author liusheding
 * @version 1.0
 * @create_date 2012-9-24
 */
public class PH {

	public static double ph = 4.0;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		while (true) {
			InputStreamReader in = new InputStreamReader(System.in);
			BufferedReader reader = new BufferedReader(in);
			String str = reader.readLine();
			ph = Double.parseDouble(str);
			if (ph == -1f)
				break;
			int natural = 0, base = 0, acid = 0;
			if (ph < 7) {
				natural = 1;
			} else if (ph > 7) {
				acid = 1;
			} else {
				base = 1;
			}
			System.out.println(natural + " " + base + " " + acid);
		}
	}

}
