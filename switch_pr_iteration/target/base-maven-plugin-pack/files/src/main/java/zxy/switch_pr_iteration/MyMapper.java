package zxy.switch_pr_iteration;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;
import java.util.Iterator;

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
       
    	key.set(0,record.getString(0));
    	
    	Double switch_pr = record.getDouble(2);
    	String[] toItems = record.getString(3).split(",");
    	for (String sss : toItems) {
			String[] itemPr = sss.split(":");
			key.set("item_to",itemPr[0]);
			
			value.set("switch_pr",switch_pr*Double.parseDouble(itemPr[1]));
			context.write(key,value);
		}
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}