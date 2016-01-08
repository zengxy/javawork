package zxy.poprandtest;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
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

		List<String> itemlist = new ArrayList<String>();
		String item_buy = "";
		class ITEMCOUNT {
			String item = "";
			Long cnt = 0L;
		}

		ITEMCOUNT[] topItems = new ITEMCOUNT[TopK];
		for (int i = 0; i < topItems.length; i++) {
			topItems[i] = new ITEMCOUNT();
		}

		while (values.hasNext()) {
			Record val = values.next();
			String item = val.getString("item");
			Long cnt = val.getBigint("cnt");
			itemlist.add(item);
			if (val.getBigint("isbought") == 1)
				item_buy = item;

			int i = 0;
			while (i < TopK) {
				if (cnt > topItems[i].cnt) {

					for (int j = topItems.length - 2; j >= i; j--) {
						topItems[j + 1].cnt = topItems[j].cnt;
						topItems[j + 1].item = topItems[j].item;
					}

					topItems[i].cnt = cnt;
					topItems[i].item = val.getString("item");
					break;
				}
				i = i + 1;
			}
		}

		if (itemlist.size() == 1)
			return;

		output.set(0, key.getBigint("session"));
		output.set(1, key.getString("user_id"));

		Random rand = new Random();
		for (int i = 0; i < TopK; i++) {
			int pos = rand.nextInt(itemlist.size());
			if (itemlist.get(pos).equals(item_buy)) {
				for (int j = i; j < topItems.length; j++)
					output.set(j + 2, 1);
				break;
			} else {
				output.set(i + 2, 0);
			}

		}

		for (int i = 0; i < topItems.length; i++) {
			if (topItems[i].item.equals(item_buy)) {
				for (int j = i; j < topItems.length; j++)
					output.set(j + 2 + TopK, 1);
				context.write(output);
				return;
			} else
				output.set(i + 2 + TopK, 0);
		}
		context.write(output);
	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}
