package zxy.comp;

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
		key.set("user_id", record.getString(0));

		value.set("item_id", record.getBigint(1).toString());
		//item大类之间存在竞争
		value.set("category",record.getString(2).substring(0, 3));
		value.set("action", record.getString(3));
		value.set("vtime", record.getString(4));
        context.write(key, value);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}