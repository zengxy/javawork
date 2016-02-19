package zxy.session;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;
import com.google.protobuf.TextFormat.ParseException;

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

	// SESSION中两个ACTION最大间隔，单位千分之一秒，用于分割SESSION
	private final long ACTION_MAX_SECONDS = 1000*3600 * 6;
	// 两次action的最短间隔，单位秒，用于过滤爬虫
	private final long ACTION_MIN_SECONDS = 3;
	// ACTION的最小长度
	private final long MIN_ACTION_LEN = 5;
	// ACTION的最大长度
	private final long MAX_ACTION_LEN = 100;
	// SESSION的最小长度
	private final long MIN_SESSION_LEN = 5;

	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		List<Object[]> rList = new ArrayList<Object[]>();
		while (values.hasNext()) {
			Record val = values.next();
			rList.add(val.toArray().clone());
		}
		

		int user_action_len = rList.size();

		if (user_action_len < MIN_ACTION_LEN)
			return;

		Collections.sort(rList, new Comparator<Object[]>() {
			public int compare(Object[] r1, Object[] r2) {
				return r1[3].toString().compareTo(r2[3].toString());
			}
		});
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Long d1 = 0L, d2 = 0L;

		int beginPos = 0;
		Long sessionID = 0L;

		try {
			d1 = sdf.parse(rList.get(0)[3].toString()).getTime();
			//output.set(0,100);
			//context.write(output);
			//System.out.println(rList.get(1)[1].toString());
			for (int i = 1; i < user_action_len; i++) {
				d2 = sdf.parse(rList.get(i)[3].toString()).getTime();
				//output.set(0,d2-d1);
				//context.write(output);
				if ((d2 - d1) > ACTION_MAX_SECONDS) {
					if ((i - beginPos) >= MIN_SESSION_LEN) {
						for (int j = beginPos; j <= i - 1; j++) {
							output.set(0, sessionID);
							output.set(1, key.get("user_id"));
							output.set(2, rList.get(j)[0].toString());
							output.set(3, rList.get(j)[1].toString());
							output.set(4, rList.get(j)[2].toString());
							output.set(5, rList.get(j)[3].toString());
							output.set(6, rList.get(j)[4].toString());
							output.set(7, rList.get(j)[5].toString());
							context.write(output);

						}
						sessionID++;
					}
					beginPos = i;
				}
				d1 = d2;
			}

			if (user_action_len + 1 - beginPos >= MIN_SESSION_LEN) {
				for (int j = beginPos; j <= user_action_len - 1; j++) {
					output.set(0, sessionID);
					output.set(1, key.get("user_id"));
					//System.out.println(rList.get(j)[0].toString());
					output.set(2, rList.get(j)[0].toString());
					output.set(3, rList.get(j)[1].toString());
					output.set(4, rList.get(j)[2].toString());
					output.set(5, rList.get(j)[3].toString());
					output.set(6, rList.get(j)[4].toString());
					output.set(7, rList.get(j)[5].toString());
					context.write(output);
				}
			}
		} catch (Exception e) {
		}
	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}
