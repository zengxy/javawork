package zxy.wyjsession;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.text.SimpleDateFormat;
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
	private final int MAX_CART_NUM = 1000;
	private final int MIN_CART_NUM = 5;
	private final int MAX_HOUR = 3;
	private final Long SESSION_GAP = MAX_HOUR * 3600 * 1000L;

	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		int action_len = 0;
		List<Object[]> rList = new ArrayList<Object[]>();
		while (values.hasNext()) {
			Record val = values.next();
			// TODO process value
			rList.add(val.toArray().clone());
			action_len += 1;
		}

		if (action_len < MIN_CART_NUM || action_len > MAX_CART_NUM)
			return;

		Collections.sort(rList, new Comparator<Object[]>() {
			public int compare(Object[] r1, Object[] r2) {
				return r1[3].toString().compareTo(r2[3].toString());
			}
		});

		Long sessionID = 0L;
		// 日期格式
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Long d1 = 0L, d2 = 0L;
		try {
			d1 = sdf.parse(rList.get(0)[3].toString()).getTime();

			for (int i = 0; i < action_len; i++) {
				d2 = sdf.parse(rList.get(i)[3].toString()).getTime();
				if (d2 - d1 > SESSION_GAP) {
					sessionID += 1;
				}

				output.set(0, sessionID);
				output.set(1, key.get("user_id"));
				int index = 2;
				for (Object r : rList.get(i)) {
					output.set(index, r.toString());
					index += 1;
				}
				context.write(output);
				d1 = d2;
			}
		} catch (Exception e) {
		}
	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}
