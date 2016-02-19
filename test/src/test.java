import java.util.HashMap;
import java.util.Map;



public class test {
	public static void main(String args[]) {

		Map<Integer, HashMap<String, String>> test = new HashMap<Integer, HashMap<String,String>>();
		
		HashMap<String, String> test1 = new HashMap<String, String>();
		
		test1.put("=aa", "==123");
		test1.put("=aaa", "==2121");

		test.put(1, test1);
		
		System.out.println(test1.get("=aaa="));
	}
}
