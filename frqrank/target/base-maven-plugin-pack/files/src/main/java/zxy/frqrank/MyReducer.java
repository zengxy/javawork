package zxy.frqrank;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
	private Record output;

	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		Map<String, Integer> frqCountMap = new HashMap<String, Integer>();
		String itemBought = "";
		List<Object[]> rList = new ArrayList<Object[]>();
		while (values.hasNext()) {
			Record val = values.next();
			String itemTemp = val.getString("item");
			if (val.getString("action").equals("alipay")) {
				itemBought = itemTemp;
				continue;
			}

			if (frqCountMap.containsKey(itemTemp)) {
				frqCountMap.put(itemTemp, frqCountMap.get(itemTemp));
			} else {
				frqCountMap.put(itemTemp, 1);
			}

			rList.add(val.toArray().clone());
		}

		int sessionLen = rList.size();
		int itemNum = frqCountMap.keySet().size();

		if (sessionLen < 4 || itemNum < 2)
			return;

		Collections.sort(rList, new Comparator<Object[]>() {
			public int compare(Object[] r1, Object[] r2) {
				return r2[2].toString().compareTo(r1[2].toString());
			}
		});

		if (frqCountMap.containsKey(itemBought) == false) {
			output.set(0, key.getBigint("session"));
			output.set(1, key.getString("user_id"));

			output.set(2, 0.99);
			output.set(3, 0.99);
			context.write(output);
			return;
		}

		output.set(0, key.getBigint("session"));
		output.set(1, key.getString("user_id"));

		int i = 0;
		while (i < sessionLen) {
			if (rList.get(i)[0].toString().equals(itemBought)) {
				output.set(2, i / (double) sessionLen);
				break;
			}
			i++;
		}

		List<Map.Entry<String, Integer>> frqList = new ArrayList<Map.Entry<String, Integer>>(
				frqCountMap.entrySet());

		Collections.sort(frqList, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
					Map.Entry<String, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		i = 0;
		while (i < itemNum) {
			if (rList.get(i)[0].toString().equals(itemBought)) {
				output.set(3, i / (double) itemNum);
				break;
			}
			i++;
		}

		context.write(output);
	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}
