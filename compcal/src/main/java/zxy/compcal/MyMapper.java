package zxy.compcal;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record key;
    private Record value;
    
    
    public void setup(TaskContext context) throws IOException {
        key = context.createMapOutputKeyRecord();
        value = context.createMapOutputValueRecord();
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
		double itemFromToPr = record.getDouble(2);
		String itemfrom=record.getString(0);
		for (String temp : record.getString(3).split(",")) {
			String[] itemAndPr = temp.split(":");
			key.set("itemfrom", itemfrom);
			key.set("itemto",itemAndPr[0]);
			
			value.set("pr",(Double.parseDouble(itemAndPr[1]))*itemFromToPr);
			context.write(key,value);
		}
		
		
        context.write(key, value);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}