package zxy.lftesttest;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
	private Record output;

	private final int TopK = 3;

	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		class ITEMUTILITY {
			String item = "";
			Double utility = Double.MIN_VALUE;
		}

		ITEMUTILITY[] items = new ITEMUTILITY[TopK];

		for (int i = 0; i < items.length; i++) {
			items[i] = new ITEMUTILITY();
		}

		Set<String> itemSet = new HashSet<String>();
		String itemBought = "";

		FeatureVector userFeatureVector = new FeatureVector(
				key.getString("userfeature"));
		while (values.hasNext()) {
			Record val = values.next();

			itemSet.add(val.getString("item"));
			if (val.getBigint("isbought") == 1)
				itemBought = val.getString("item");

			FeatureVector itemFeatureVector = new FeatureVector(
					val.getString("itemfeature"));
			Double utility = FeatureVector.innerProduct(userFeatureVector,
					itemFeatureVector);

			int i = 0;
			while (i < TopK) {
				if (utility > items[i].utility) {

					for (int j = items.length - 2; j >= i; j--) {
						items[j + 1].utility = items[j].utility;
						items[j + 1].item = items[j].item;
					}

					items[i].utility = utility;
					items[i].item = val.getString("item");
					break;
				}
				i = i + 1;
			}

		}

		if (itemSet.size() == 1)
			return;

		output.set(0, key.getBigint("session"));
		output.set(1, key.getString("user_id"));
		for (int i = 0; i < items.length; i++) {
			if (items[i].item.equals(itemBought)) {
				for (int j = i; j < items.length; j++)
					output.set(j + 2, 1);
				context.write(output);
				return;
			} else
				output.set(i + 2, 0);
		}
		context.write(output);
	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}
