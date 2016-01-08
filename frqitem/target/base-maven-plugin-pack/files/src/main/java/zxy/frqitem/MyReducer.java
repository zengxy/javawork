package zxy.frqitem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

public class MyReducer extends ReducerBase {
	private Record output;

	@Override
	public void setup(TaskContext context) throws IOException {
		super.setup(context);
		output = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		List<String> items=new ArrayList<String>();
		while (values.hasNext()) {
			Record val = values.next();
			// TODO process value
			items.add(val.getString("itemc"));
		}

		Collections.sort(items);
		
		output.set(0,key.getString("item"));
		output.set(1,StringUtils.join(items,"&"));
		context.write(output);
		
	}
}
