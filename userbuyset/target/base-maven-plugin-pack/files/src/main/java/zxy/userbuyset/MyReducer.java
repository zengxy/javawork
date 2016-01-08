package zxy.userbuyset;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

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

		Set<String> buyItemSet = new HashSet<String>();

		while (values.hasNext()) {
			Record val = values.next();
			buyItemSet.add(val.getString("item"));
		}

		String buyItems = StringUtils.join(buyItemSet, "&");

		for (String item : buyItemSet) {
			output.set(0, key.getString("user_id"));
			output.set(1, buyItems);
			output.set(2, item);
			context.write(output);
		}

	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}
