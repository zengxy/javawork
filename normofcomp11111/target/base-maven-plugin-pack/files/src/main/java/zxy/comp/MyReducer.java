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
				return r1[2].toString().compareTo(r2[2].toString());
			}
		});
		
		int rListLen=rList.size();
		if (rListLen<10)
			return;
		
		for (int i = 0; i < rListLen-1; i++) {
			
			output.set(0,rList.get(i)[0].toString());
			output.set(1,rList.get(i+1)[0].toString());
			
			context.write(output);
		}
	
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}