package zxy.lfrstate1;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.Reducer.TaskContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
	// 正则化参数
	final private double lambda = 0.01;
	// 迭代速率
	final private double stepRate = 0.05;
	// 特征维度
	final private int featureLen = 5;

	private Record output;

	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		Map<Long, String> sessionOcItem = new HashMap<Long, String>();
		Map<Long, Double> sessionOcFuis = new HashMap<Long, Double>();
		Map<Long, String> sessionOcFeature = new HashMap<Long, String>();

		Map<Long, String> sessionBuyItem = new HashMap<Long, String>();
		Map<Long, Double> sessionBuyFuis = new HashMap<Long, Double>();
		Map<Long, String> sessionBuyFeature = new HashMap<Long, String>();

		List<Object[]> rList = new ArrayList<Object[]>();

		String userfeatureString = "";
		Random rand = new Random();
		while (values.hasNext()) {
			Record val = values.next();
			rList.add(val.toArray().clone());
			userfeatureString = val.getString("userfeature");

			Long session = val.getBigint("session");
			String item_id = val.getString("item");
			Double fuis = val.getDouble("fuis");
			String itemFeature = val.getString("itemfeature");
			if (val.getBigint("isbought") == 1) {
				sessionBuyItem.put(session, item_id);
				sessionBuyFuis.put(session, fuis);
				sessionBuyFeature.put(session, itemFeature);
			}

			else {

				if (!sessionOcFuis.containsKey(session)
						|| rand.nextBoolean() == true) {
					sessionOcItem.put(session, item_id);
					sessionOcFuis.put(session, fuis);
					sessionOcFeature.put(session, itemFeature);
				}
			}
		}

		if (sessionOcItem.isEmpty())
			return;

		FeatureVector duf = new FeatureVector(featureLen, 0.0);
		FeatureVector userFeatureVector = new FeatureVector(userfeatureString);
		for (Long session : sessionOcItem.keySet()) {
			FeatureVector ocItemFeatureVector = new FeatureVector(
					sessionOcFeature.get(session));
			FeatureVector buyItemFeatureVector = new FeatureVector(
					sessionBuyFeature.get(session));
			duf.selfIncrease(FeatureVector.featureSub(ocItemFeatureVector,
					buyItemFeatureVector));
		}

		userFeatureVector.featureMultiply(1 - lambda);
		userFeatureVector.selfDecrease(duf);

		for (Object[] objects : rList) {
			Long session = Long.parseLong(objects[1].toString());

			output.set(0, session);// session
			output.set(1, key.getString("user_id"));
			output.set(2, userFeatureVector.getFeatureString());
			Long isbought = Long.parseLong(objects[2].toString());
			String item = objects[3].toString();
			output.set(3, isbought);
			output.set(4, item);
			output.set(5, Double.parseDouble(objects[4].toString()));
			output.set(6, objects[5].toString());
			context.write(output);
		}

	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}