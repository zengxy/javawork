package zxy.utilitytest;

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
    	key=context.createMapOutputKeyRecord();
    	value=context.createMapOutputValueRecord();
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
    	//key.set("user_id",record.getString(1));
    	//key.set("session",record.getBigint(0));
    	
    	value.set("item_id",record.getBigint(2).toString());
    	//value.set("vtime",record.getString(5));
    	
    	context.write(key,value);
    }

	@Override
	public void cleanup(TaskContext arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
}