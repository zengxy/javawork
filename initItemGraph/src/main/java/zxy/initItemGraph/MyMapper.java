package zxy.initItemGraph;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */

//以brand作为comp的branch
public class MyMapper implements Mapper {
	private Record key;
	private Record value;

	public void setup(TaskContext context) throws IOException {
		key = context.createMapOutputKeyRecord();
		value = context.createMapOutputValueRecord();
	}

	public void map(long recordNum, Record record, TaskContext context)
			throws IOException {
		double brandToSessionPr = record.getDouble(2);
		String brandfrom=record.getString(0);
		for (String temp : record.getString(3).split(",")) {
			String[] brandAndPr = temp.split(":");
			key.set("brandfrom",brandfrom);
			key.set("brandto",brandAndPr[0]);
			
			value.set("pr",(Double.parseDouble(brandAndPr[1]))*brandToSessionPr);
			context.write(key,value);
		}
	}

	public void cleanup(TaskContext context) throws IOException {

	}
}