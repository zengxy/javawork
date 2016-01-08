package zxy.frqauc;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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

		Set<String> buyBundleSet = new HashSet<String>();
		Set<String> nobuyBundleSet = new HashSet<String>();

		Map<String, Integer> bundleCount = new HashMap<String, Integer>();

		// groundtruth pair
		Set<String> AUCpair = new HashSet<String>();
		// test pair
		Set<String> AUCtestpair = new HashSet<String>();

		while (values.hasNext()) {
			Record val = values.next();
			String item = val.getString("item_id");
			String action = val.getString("action");
			for (String itemc : val.getString("bundle").split("&")) {
				String bundle;

				// 得到bundle string
				if (val.getString("item_id").compareTo(itemc) < 0)
					bundle = item + "&" + itemc;
				else
					bundle = itemc + "&" + item;

				// 计数
				if (bundleCount.containsKey(bundle))
					bundleCount.put(bundle, bundleCount.get(bundle) + 1);
				else {
					bundleCount.put(bundle, 0);
				}

				if (action.equals("alipay")) {
					buyBundleSet.add(bundle);
					bundleCount.put(bundle, bundleCount.get(bundle) - 1);
				}
			}
		}

		// 没有买，或者只有买，return
		if (buyBundleSet.size() == 0
				|| buyBundleSet.size() == bundleCount.keySet().size())
			return;

		nobuyBundleSet.addAll(bundleCount.keySet());
		nobuyBundleSet.removeAll(buyBundleSet);

		for (String buyBundle : buyBundleSet)
			for (String noBuyBundle : nobuyBundleSet) {
				AUCpair.add(buyBundle + ">" + noBuyBundle);
			}

		for (String b1 : bundleCount.keySet()) {
			for (String b2 : bundleCount.keySet()) {
				if (bundleCount.get(b1) > bundleCount.get(b2)) {
					AUCtestpair.add(b1 + ">" + b2);
				}
			}
		}

		Long truth = 0L;
		for (String groundtruth : AUCpair) {
			if (AUCtestpair.contains(groundtruth)) {
				truth += 1;
			}
		}

		output.set(0, key.getBigint("session"));
		output.set(1, key.getString("user_id"));
		output.set(2, truth);
		output.set(3, AUCpair.size());

		context.write(output);
	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}
