package zxy.compcal;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record output;
    private final double alpha=0.15;
    public void setup(TaskContext context) throws IOException {
        output = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
    	double switch_pr=0;
    	while (values.hasNext()) {
            Record val = values.next();
            switch_pr+=val.getDouble("pr");
        }
		output.set(0, key.getString("itemfrom"));
		output.set(1, key.getString("itemto"));
		
		if(key.getString("itemfrom").equals(key.getString("itemto")))
			switch_pr = (1-alpha)*switch_pr + alpha;
		else
			switch_pr = (1-alpha)*switch_pr;
			
        output.set(2, switch_pr);
        context.write(output);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
