package zxy.frqitem;

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
        String bundle = record.getString(0);
        String[] items=bundle.split("&");
        
        key.set("item",items[0]);
        value.set("itemc",items[1]);
        context.write(key,value);
        
        key.set("item",items[1]);
        value.set("itemc",items[0]);
        context.write(key,value); 
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}