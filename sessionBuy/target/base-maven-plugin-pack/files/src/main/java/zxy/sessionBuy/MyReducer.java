package zxy.sessionBuy;

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
	// session最大和最小长度
	private final int MAX_SESSION_LEN = 1000;
	private final int MIN_SESSION_LEN = 5;
	// 间隔
	private final int MAX_SESSION_HOUR = 12;
	private final Long MAX_SESSION_TIME = MAX_SESSION_HOUR * 3600 * 1000L;

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

		if (action_len < MIN_SESSION_LEN)
			return;

		Collections.sort(rList, new Comparator<Object[]>() {
			public int compare(Object[] r1, Object[] r2) {
				return r1[3].toString().compareTo(r2[3].toString());
			}
		});

		/*
		for (Object[] tempObjects : rList) {
			output.set(0, 0);
			output.set(1, key.get("user_id"));
			output.set(2, Long.parseLong(tempObjects[0]
					.toString()));
			output.set(3, tempObjects[1].toString());
			output.set(4, tempObjects[2].toString());
			output.set(5, tempObjects[3].toString());
			context.write(output);
		}
		
		
		if (true) {
			return;
		}
		*/
		
		List<Integer> buyPosList = new ArrayList<Integer>();
		for (int i = 0; i < action_len; i++)
			if (rList.get(i)[2].toString().equals("alipay"))
				buyPosList.add(i);

		// 日期格式
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Long d1 = 0L, d2 = 0L;

		int beginPos = 0;
		Long sessionID = 0L;

		for (Integer endPos : buyPosList) {
			if ((endPos - beginPos + 1) >= MIN_SESSION_LEN) {
				try {
					d2 = sdf.parse(rList.get(endPos)[3].toString()).getTime();

					for (int i = endPos - 1; i >= beginPos; i--) {
						d1 = sdf.parse(rList.get(i)[3].toString()).getTime();
						if (i == beginPos
								|| ((d2 - d1) > MAX_SESSION_TIME && (endPos - i) >= MIN_SESSION_LEN)) {

							if (i == beginPos && (d2 - d1) < MAX_SESSION_TIME)
								i -= 1;
							
							if((endPos - i) < MIN_SESSION_LEN)
								break;
							
							for (int j = i + 1; j <= endPos; j++) {
								Object[] tempObjects = rList.get(j);
								output.set(0, sessionID);
								output.set(1, key.get("user_id"));
								output.set(2, Long.parseLong(tempObjects[0]
										.toString()));
								output.set(3, tempObjects[1].toString());
								output.set(4, tempObjects[2].toString());
								output.set(5, tempObjects[3].toString());
								context.write(output);
							}
							sessionID += 1;
							break;
						}

						else if ((d2 - d1) < MAX_SESSION_TIME) {
							d2 = d1;
						} else {
							break;
						}
					}

				} catch (Exception e) {
				}

			}

			beginPos = endPos + 1;
		}
	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}
