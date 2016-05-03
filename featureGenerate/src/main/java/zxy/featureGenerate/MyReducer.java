package zxy.featureGenerate;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record output;

    public void setup(TaskContext context) throws IOException {
        output = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
    
		List<Object[]> rList = new ArrayList<Object[]>();
		while (values.hasNext()) {
			Record val = values.next();	
			// TODO process value
			rList.add(val.toArray().clone());
		} 
				
		Collections.sort(rList, new Comparator<Object[]>() {
			public int compare(Object[] r1, Object[] r2) {
				return r1[2].toString().compareTo(r2[2].toString());
			}
		});
		
		Map<String, Double> itemFrqMap = new HashMap<String, Double>();
		Map<String,Double> itemRenMap = new HashMap<String, Double>();
		
		int sessionSize = rList.size();
		
		for (int i = 0; i < sessionSize-1; i++) {
			Object[] temp = rList.get(i);
			if (itemFrqMap.keySet().contains(temp[0].toString())) {
				itemFrqMap.put(temp[0].toString(),itemFrqMap.get(temp[0].toString())+1);
			}
			else {
				itemFrqMap.put(temp[0].toString(),1.0);
			}
			

			itemRenMap.put(temp[0].toString(),sessionSize-1.0-i);
		}
		
		String itemBuy = rList.get(sessionSize-1)[0].toString();
		
		for(String item:itemFrqMap.keySet()){
			
			System.out.println(itemFrqMap.get(item));
			
			output.set(0,key.get("session"));
			output.set(1,key.get("user_id"));
			output.set(2,item);
			
			output.set(3,itemFrqMap.get(item));
			output.set(4,itemFrqMap.get(item)/(sessionSize-1));
			output.set(5,itemRenMap.get(item));
			output.set(6,itemRenMap.get(item)/(sessionSize-1));
			
			if (item.equals(itemBuy)) {
				output.set(7,1);
			}
			else {
				output.set(7,0);
			}
			
			context.write(output);
		}
    	
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
