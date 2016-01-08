package zxy.usersim;

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
        key.set("u", record.getString(0));
        key.set("bs", record.getString(1));
        
        value.set("uc",record.getString(2));
        value.set("bsc",record.getString(3));
        value.set("cnt",record.getBigint(4));
        context.write(key, value);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}