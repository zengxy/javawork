package zxy.comp;

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
    final int MIN_TO_ITEM_NUM = 5;
    public void setup(TaskContext context) throws IOException {
        output = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
    	
		List<Object[]> rList = new ArrayList<Object[]>();
        while (values.hasNext()) {
            Record val = values.next();
			rList.add(val.toArray().clone()); 
        }
        
		Collections.sort(rList, new Comparator<Object[]>() {
			public int compare(Object[] r1, Object[] r2) {
				return r1[3].toString().compareTo(r2[3].toString());
			}
		});
		
		int rListLen=rList.size();
		if (rListLen < MIN_TO_ITEM_NUM)
			return;
		
		for (int i = 0; i < rListLen-1; i++) {
			
			if ( ! rList.get(i)[1].equals(rList.get(i+1)[1]) )
				continue;
			//1是category，只考虑相同item之间的对比
			output.set(0,rList.get(i)[0].toString());
			output.set(1,rList.get(i+1)[0].toString());
			
			context.write(output);
		}
	
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
