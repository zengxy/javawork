package zxy.usersim;

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

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
	private Record output;
	private final String itemRegex = "&";

	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		Map<String, Double> itemSim = new HashMap<String, Double>();

		// Set<String> uitemSet = new HashSet<String>();
		// Collections.addAll(uitemSet, key.getString("bs").split(itemRegex));
		int uItemNum = key.getString("bs").split(itemRegex).length;

		while (values.hasNext()) {
			Record val = values.next();
			String[] ucitems = val.getString("bsc").split(itemRegex);
			int ucItemNum = ucitems.length;

			Double uucSim = (0.0 + val.getBigint("cnt"))
					/ (ucItemNum + uItemNum - val.getBigint("cnt"));

			for (String item : ucitems) {
				if (itemSim.containsKey(item)) {
					itemSim.put(item, itemSim.get(item) + uucSim);
				} else {
					itemSim.put(item, uucSim);
				}
			}
		}

		List<Map.Entry<String, Double>> itemList = new ArrayList<Map.Entry<String, Double>>(
				itemSim.entrySet());
		Collections.sort(itemList, new Comparator<Map.Entry<String, Double>>() {
			@Override
			public int compare(Map.Entry<String, Double> arg0,
					Map.Entry<String, Double> arg1) {
				return arg1.getValue().compareTo(arg0.getValue());
			}
		});

		String itemsString = itemList.get(0).getKey();

		for (int i = 1; i < itemList.size(); i++)
			itemsString = itemsString + "," + itemList.get(i).getKey();

		output.set(0, key.get(0));
		output.set(1, itemsString);
		context.write(output);
	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}
