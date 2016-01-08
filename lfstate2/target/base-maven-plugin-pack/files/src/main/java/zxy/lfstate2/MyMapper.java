package zxy.lfstate2;

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
    	
		key.set("item", record.getString(4));
		
		value.set("session", record.getBigint(0));
		value.set("user_id",record.getString(1));
		value.set("userfeature", record.getString(2));
		value.set("isbought", record.getBigint(3));
		value.set("fuis", record.getDouble(5));
		value.set("itemfeature", record.getString(6));
		
		
		context.write(key,value);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}