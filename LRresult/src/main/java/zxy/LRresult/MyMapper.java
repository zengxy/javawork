package zxy.LRresult;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Mapper.TaskContext;

import java.io.IOException;
import java.util.Random;

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
    	Random random = new Random();
        key.set(0, record.getBigint(0));
        key.set(1, record.getString(1));

        value.set(0,record.get(2));
        value.set(1,record.get(3));
        value.set(2,record.get(4));
        value.set(3,record.get(5));
        value.set(4,record.get(6));
        value.set(5,record.get(7));

        context.write(key, value);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}