package zxy.knnresult;

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
	private final String regex = ",";
	private final int TopK = 3;

	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		String itemBought = "";
		String[] itemStrings = key.getString("itemsrank").split(regex);

		Map<String, Integer> itemRankMap = new HashMap<String, Integer>();
		for (int i = 0; i < itemStrings.length; i++) {
			itemRankMap.put(itemStrings[i], i);
		}

		class ITEMRANK {
			String item = "";
			int rank = Integer.MAX_VALUE;
		}

		ITEMRANK[] items = new ITEMRANK[TopK];
		for (int i = 0; i < items.length; i++) {
			items[i] = new ITEMRANK();
		}

		while (values.hasNext()) {
			Record val = values.next();
			String itemTemp = val.getString("item");
			if (val.getBigint("isbought") == 1)
				itemBought = itemTemp;

			if (itemRankMap.containsKey(itemTemp) == false)
				continue;

			int rank = itemRankMap.get(itemTemp);
			int i = 0;
			while (i < TopK) {
				if (rank < items[i].rank) {

					for (int j = items.length - 2; j >= i; j--) {
						items[j + 1].rank = items[j].rank;
						items[j + 1].item = items[j].item;
					}

					items[i].rank = rank;
					items[i].item = itemTemp;
					break;
				}
				i = i + 1;
			}
		}

		output.set(0, key.getBigint("session"));
		output.set(1, key.getString("user_id"));
		for (int i = 0; i < TopK; i++) {
			if (items[i].item.equals(itemBought)) {
				for (int j = i; j < TopK; j++)
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
