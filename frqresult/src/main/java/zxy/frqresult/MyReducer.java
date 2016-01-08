package zxy.frqresult;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import java.util.Iterator;
import java.util.List;

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
		Boolean isbuy = false;
		while (values.hasNext()) {
			Record val = values.next();
			if (val.getString("action").equals("alipay"))
				isbuy = true;
			rList.add(val.toArray().clone());
		}

		if (isbuy == false) {
			return;
		}

		Collections.sort(rList, new Comparator<Object[]>() {
			public int compare(Object[] r1, Object[] r2) {
				return r1[3].toString().compareTo(r2[3].toString());
			}
		});

		output.set(0, key.getBigint("session"));
		output.set(1, key.getString("user_id"));

		for (int i = rList.size() - 1; i >= 0; i--) {
			// 定位到buy
			if (rList.get(i)[2].toString().equals("alipay")) {
				String buyitem = rList.get(i)[0].toString();
				if ((i - 1) >= 0) {
					if (rList.get(i - 1)[0].toString().equals(buyitem)) {
						output.set(2, 1);
						output.set(3, 1);
						output.set(4, 1);
						output.set(5, 1);
						context.write(output);
						return;
					}

					else if (rList.get(i - 1)[1].toString().indexOf(buyitem) > 0) {
						output.set(2, 0);
						output.set(3, 1);
						output.set(4, 1);
						output.set(5, 1);
						context.write(output);
						return;
					}

					else if (i - 2 >= 0) {

						if (rList.get(i - 2)[0].toString().equals(buyitem)) {
							output.set(2, 0);
							output.set(3, 0);
							output.set(4, 1);
							output.set(5, 1);
							context.write(output);
							return;
						}

						else if (rList.get(i - 2)[1].toString()
								.indexOf(buyitem) > 0) {
							output.set(2, 0);
							output.set(3, 0);
							output.set(4, 0);
							output.set(5, 1);
							context.write(output);
							return;
						}
					}
				}

				output.set(2, 0);
				output.set(3, 0);
				output.set(4, 0);
				output.set(5, 0);
				context.write(output);
				return;
			}
		}
	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}
