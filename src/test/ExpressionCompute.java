package test;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * @author liusheding
 * @version 1.0
 * @create_date 2012-9-26
 */
public class ExpressionCompute {
	
	private Stack<Character> infix;
	private Stack<String> postfix;
	private List<String> list;
	private String original;

	public ExpressionCompute(String src) {
		original = src;
		infix = new Stack<Character>();
		postfix = new Stack<String>();
		list = new ArrayList<String>();
		initInfix();
	}

	public String getResult() {
		for (int i = 0; i < list.size(); i++) {
			String tmp = list.get(i);
			if (!tmp.equals("(")) {
				if (tmp.equals("+") || tmp.equals("-") || tmp.equals("*")
						|| tmp.equals("/") || tmp.equals("%")) {
					double a = Double.parseDouble(postfix.pop());
					double b = Double.parseDouble(postfix.pop());
					Double r = compute(b, a, tmp.charAt(0));
					postfix.push(r.toString());
				} else {
					postfix.push(tmp);
				}
			}
		}
		return postfix.pop();
	}

	private List<String> initInfix() {
		int k = 0;
		for (int i = 0; i < original.length(); i++) {
			char ch = original.charAt(i);
			if ((ch < '0' || ch > '9') && ch != '.') {
				String number = original.substring(k, i);
				if (number.length() > 0) {
					list.add(number);
				}
				switch (ch) {
				case '(':
					infix.push(ch);
					break;
				case ')':
					while (!infix.isEmpty() && infix.peek() != '(') {
						list.add(infix.pop() + "");
					}
					break;
				case '*':
				case '/':
				case '%':
					infix.push(ch);
					break;
				case '+':
				case '-':
					while (!infix.isEmpty()
							&& (getLevel(ch) <= getLevel(infix.peek()))) {
						list.add(infix.pop() + "");
					}
					infix.push(ch);
					break;
				}
				k = i + 1;
			}
		}
		if(original.substring(k).length() > 0){
			list.add(original.substring(k));
		}
		while (!infix.isEmpty()) {
			list.add(infix.pop().toString());
		}
		return list;
	}

	private int getLevel(char ch) {
		if (ch == '*' || ch == '/' || ch == '%') {
			return 2;
		} else if (ch == '+' || ch == '-') {
			return 1;
		} else {
			return 0;
		}
	}

	public double compute(double a, double b, char operator) {
		double result;
		switch (operator) {
		case '+':
			result = a + b;
			break;
		case '-':
			result = a - b;
			break;
		case '*':
			result = a * b;
			break;
		case '/':
			result = a / b;
			break;
		case '%':
			result = a % b;
			break;
		default:
			result = 0f;
			break;
		}
		return result;
	}
	
	public void printInfix(){
		for (int i = 0; i < list.size(); i++){
			System.out.print(list.get(i)+" ");
		}
		System.out.print("\n");
	}
	
	public static void main(String[] args) throws InterruptedException {
		ExpressionCompute stack = new ExpressionCompute(args[0]); // 2 5 2 - 3 * ( +
		stack.printInfix();
		System.out.println(stack.getResult());
	}
}