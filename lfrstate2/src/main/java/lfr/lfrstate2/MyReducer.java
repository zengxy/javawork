package lfr.lfrstate2;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.Reducer.TaskContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
	private Record output;

	// 正则化参数
	final private double lambda = 0.01;
	// 迭代速率
	final private double stepRate = 0.05;
	// 特征维度
	final private int featureLen = 5;

	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {
		FeatureVector di = new FeatureVector(featureLen, 0.0);
		String itemfeatureString = "";
		List<Object[]> rList = new ArrayList<Object[]>();
		while (values.hasNext()) {
			Record val = values.next();
			rList.add(val.toArray().clone());

			if (val.getBigint("isbought") == 1)
				di.selfDecrease(new FeatureVector(val.getString("userfeature")));
			else
				di.selfIncrease(new FeatureVector(val.getString("userfeature")));

			itemfeatureString = val.getString("itemfeature");
		}
		di.featureMultiply(stepRate);
		FeatureVector itemFeatureVector = new FeatureVector(itemfeatureString);
		itemFeatureVector.featureMultiply(1 - lambda * stepRate);
		itemFeatureVector.selfDecrease(di);

		for (Object[] objects : rList) {
			output.set(0, Long.parseLong(objects[0].toString()));// session
			output.set(1, objects[1].toString());
			output.set(2, objects[2].toString());
			output.set(3, Long.parseLong(objects[3].toString()));
			output.set(4, key.getString("item"));
			output.set(5, Double.parseDouble(objects[4].toString()));
			output.set(6, objects[5].toString());
			context.write(output);
		}
	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}