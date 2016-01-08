package zxy.frqq;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

		Map<String, Integer> bundle_count = new HashMap<String, Integer>();
		String buyitem = "";
		Boolean isbuy = false;
		while (values.hasNext()) {
			Record val = values.next();
			// TODO process value
			String item = val.getString("item_id");
			String action = val.getString("action");

			if (action.equals("alipay")) {
				buyitem = item;
				isbuy = true;
			}

			else {
				// 更新bundle的计数
				for (String bundle : bundle_count.keySet()) {
					if (bundle.indexOf(item) >= 0)
						bundle_count.put(bundle, bundle_count.get(bundle) + 1);
				}

				for (String itemc : val.getString("bundle").split("&")) {
					String bundle;
					if (item.compareTo(itemc) < 0)
						bundle = item + "&" + itemc;
					else
						bundle = itemc + "&" + item;

					if (!bundle_count.containsKey(bundle))
						bundle_count.put(bundle, 1);

				}
			}
		}
		if (isbuy == false) {
			return;
		}

		if (bundle_count.size() == 0)
			return;

		int max_count = 0;
		String max_bundle = "";
		for (String bundle : bundle_count.keySet()) {
			if (bundle_count.get(bundle) > max_count) {
				max_count = bundle_count.get(bundle);
				max_bundle = bundle;
			}
		}

		// if (max_item.equals(""))
		// return;

		output.set(0, key.getBigint("session"));
		output.set(1, key.getString("user_id"));
		if (max_bundle.indexOf(buyitem) >= 0)
			output.set(2, 1);
		else
			output.set(2, 0);

		context.write(output);

	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}
