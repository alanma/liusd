package test;

import hadoop.jdbc.model.Cups;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.jruby.compiler.ir.targets.JDK6;

/**
 * @author liusheding
 * @version 1.0
 * @create_date 2012-9-25
 */
public class AssertTest {
	
	private final static String[] cFields = {"priAcctNoConv","settleAt"};
	
	public static void main(String[] args) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, NoSuchFieldException {
		AssertTest at = new AssertTest();
		System.out.println(at.getClass().getClassLoader());
		try {
			at.assertMe(true);
			at.assertMe(false);
		} catch (AssertionError ae) {
			System.out.println("AsseriontError catched");
		}
		Cups cups = new Cups();
		for (int i = 0; i < cFields.length; i++) {
			Field field = cups.getClass().getDeclaredField(cFields[i]);
			System.out.println(field.getType());
			field.set(cups, null);
		}
		String a = "dsa";
		System.out.println("go on");
		toString("a","b","asd");
		Method toMethod = AssertTest.class.getDeclaredMethod("toString",String[].class);
		System.out.println(toMethod.getReturnType());
		Object o = toMethod.invoke(new AssertTest(), new Object[] { new String[] {"method"} });
		System.out.println(o);
		Method[] methods = AssertTest.class.getDeclaredMethods();
		for (Method method : methods) {
			if (method.getName().equals("main")) {
				method.invoke(new AssertTest(), new Object[]{ new String[]{""}});
			}
		}
	}
	
	public static void toString(String... strings) {
		System.out.println(strings.length);
	}

	private void assertMe(boolean boo) {
		assert boo ? true : false;
		System.out.println("true condition");
		// System.console().printf("s", "a");
	}
}
