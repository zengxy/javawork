package zxy.utilitytest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

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
		
		output.set(0,key.getString("item"));
		FeatureVector itemFeatureVector=new FeatureVector(5);
		output.set(1,itemFeatureVector.getFeatureString());
		context.write(output);
		
		
		/*
		List<Object[]> rList = new ArrayList<Object[]>();
		Map<String, Double> itemUtilityMap = new HashMap<String, Double>();
		int sessionLen = 0;
		while (values.hasNext()) {
			Record val = values.next();
			// TODO process value
			rList.add(val.toArray().clone());
			itemUtilityMap.put(val.getString("item_id"), 0.0);
			sessionLen += 1;
		}

		// 顺序排列
		Collections.sort(rList, new Comparator<Object[]>() {
			public int compare(Object[] r1, Object[] r2) {
				return r1[1].toString().compareTo(r2[1].toString());
			}
		});

		for (int i = 0; i < sessionLen - 1; i++) {
			String itemTemp = rList.get(i)[0].toString();
			itemUtilityMap.put(itemTemp, itemUtilityMap.get(itemTemp)
					+ sessionUtilityFunction(i, sessionLen));
		}
		String itemBought=rList.get(sessionLen - 1)[0].toString();
		
		output.set(0, key.getString("user_id"));
		output.set(1, key.getBigint("session"));
		
		for (String item : itemUtilityMap.keySet()) {
			if(item.equals(itemBought))
				output.set(2, 1);
			else
				output.set(2,0);
			output.set(3, item);
			output.set(4, itemUtilityMap.get(item));
			FeatureVector itemFeatureVector=new FeatureVector(5);
			output.set(5,itemFeatureVector.getFeatureString());
			context.write(output);
		}
		*/
		/*
		 * for only f test double maxUtility = 0; String maxItem = "";
		 * 
		 * double max2Utility = 0; String max2Item = "";
		 * 
		 * 
		 * 
		 * for (String item : itemUtilityMap.keySet()) { Double utilityTemp =
		 * itemUtilityMap.get(item); if (maxUtility < utilityTemp) { max2Utility
		 * = maxUtility; max2Item = maxItem;
		 * 
		 * maxUtility = utilityTemp; maxItem = item; } /* for local test
		 * output.set(0, key.getString("user_id")); output.set(1,
		 * key.getBigint("session")); output.set(2, item); output.set(3,
		 * utilityTemp); context.write(output);
		 * 
		 * }
		 * 
		 * output.set(0, key.getString("user_id")); output.set(1,
		 * key.getBigint("session")); String buyItem = rList.get(sessionLen -
		 * 1)[0].toString(); if (maxItem.equals(buyItem) ||
		 * max2Item.equals(buyItem)) output.set(2, 1); else output.set(2, 0);
		 * 
		 * context.write(output);
		 */
	}

	private Double sessionUtilityFunction(int i, int sessionLen) {
		// TODO Auto-generated method stub
		return Math.exp(-(double) (sessionLen - i) / sessionLen);
	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}
