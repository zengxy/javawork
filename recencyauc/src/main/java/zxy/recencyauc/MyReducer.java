package zxy.recencyauc;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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

		List<Object[]> rList = new ArrayList<Object[]>();
		Set<String> buyBundleSet = new HashSet<String>();
		Set<String> nobuyBundleSet = new HashSet<String>();
		Set<String> allBundleSet = new HashSet<String>();

		// item -> bundle 的映射
		Map<String, Set<String>> itemBundleMap = new HashMap<String, Set<String>>();

		// groundtruth pair
		Set<String> AUCpair = new HashSet<String>();
		// test pair
		Set<String> AUCtestpair = new HashSet<String>();

		while (values.hasNext()) {
			Record val = values.next();
			String item = val.getString("item_id");
			String action = val.getString("action");
			Set<String> itemBundleSet = new HashSet<String>();
			for (String itemc : val.getString("bundle").split("&")) {
				String bundle;
				if (val.getString("item_id").compareTo(itemc) < 0)
					bundle = item + "&" + itemc;
				else
					bundle = itemc + "&" + item;

				itemBundleSet.add(bundle);
				allBundleSet.add(bundle);

				if (action.equals("alipay")) {
					buyBundleSet.add(bundle);
				}
			}

			if (!itemBundleMap.containsKey(item))
				itemBundleMap.put(item, itemBundleSet);

			rList.add(val.toArray().clone());
		}

		// 没有买，或者只有买，return
		if (buyBundleSet.size() == 0
				|| buyBundleSet.size() == allBundleSet.size())
			return;

		nobuyBundleSet.addAll(allBundleSet);
		nobuyBundleSet.removeAll(buyBundleSet);

		for (String buyBundle : buyBundleSet)
			for (String noBuyBundle : nobuyBundleSet) {
				AUCpair.add(buyBundle + ">" + noBuyBundle);
			}

		Collections.sort(rList, new Comparator<Object[]>() {
			public int compare(Object[] r1, Object[] r2) {
				return r1[3].toString().compareTo(r2[3].toString());
			}
		});

		List<String> itemRecencyList = new ArrayList<String>();
		Set<String> itemRecencySet = new HashSet<String>();

		for (int i = rList.size() - 1; i >= 1; i--) {
			if (rList.get(i)[2].toString().equals("alipay"))
				continue;

			String itemsss = rList.get(i)[0].toString();
			if (itemRecencySet.contains(itemsss))
				continue;

			else {
				itemRecencyList.add(itemsss);
				itemRecencySet.add(itemsss);
			}
		}

		for (int i = 0; i < itemRecencyList.size() - 1; i++) {
			// 大bundle set
			Set<String> bigBundleSet = itemBundleMap.get(itemRecencyList.get(i));

			for (int j = i + 1; j < itemRecencyList.size(); j++) {
				Set<String> littleBundleSet = itemBundleMap.get(itemRecencyList.get(j));
				// 加入testAUC中
				for (String bigBundle : bigBundleSet)
					for (String littleBundle : littleBundleSet) {
						AUCtestpair.add(bigBundle + ">" + littleBundle);
					}
			}
		}

		Long truth = 0L;
		for (String groundtruth : AUCtestpair) {
			if (AUCpair.contains(groundtruth)) {
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
