package zxy.normofcomp;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record output;
    final int MIN_SWITCH_COUNT=1;

    public void setup(TaskContext context) throws IOException {
        output = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
    	
		Map<String, Long> item_count_map = new HashMap<String, Long>();
		Long count_sum=0L;
		while (values.hasNext()) {
            Record val = values.next();
            count_sum = count_sum + val.getBigint("cnt");
            item_count_map.put(val.getString("item_to"),val.getBigint("cnt"));
		}
		
		if(count_sum<MIN_SWITCH_COUNT)
			return;
		
		output.set(0,key.getString("item_from"));
		for (String item_to: item_count_map.keySet()) {			
			Long cnt=item_count_map.get(item_to);  
			output.set(1,item_to);
			output.set(2,(double)cnt/count_sum);
			output.set(3,cnt);
			context.write(output);
		}
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
