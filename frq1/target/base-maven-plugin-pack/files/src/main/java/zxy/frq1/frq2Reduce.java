package zxy.frq1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

public class frq2Reduce extends ReducerBase {
	private Record output;

	@Override
	public void setup(TaskContext context) throws IOException {
		super.setup(context);
		output = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		Set<String> item_set = new HashSet<String>();

		while (values.hasNext()) {
			Record val = values.next();
			// TODO process value
			item_set.add(val.getString("item_id"));
		}

		if (item_set.size() <= 1)
			return;

		List<String> item_list = new ArrayList<String>(item_set);
		Collections.sort(item_list);

		for (int i = 0; i < item_list.size() - 1; i++)
			for (int j = i + 1; j < item_list.size(); j++) {
				output.set(0, item_list.get(i) + "&" + item_list.get(j));
				output.set(1, 1);
				context.write(output);
			}
	}
}
