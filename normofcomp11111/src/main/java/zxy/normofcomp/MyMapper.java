package zxy.normofcomp;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record key;
    private Record value;
    final Long MIN_SWITCH_COUNT=10L;

    public void setup(TaskContext context) throws IOException {
        key = context.createMapOutputKeyRecord();
        value = context.createMapOutputValueRecord();
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
    	Long cnt=record.getBigint(2);
    	if(cnt<10)
    		return;
		key.set("item_from", record.getString(0));

		value.set("item_to", record.getString(1));
		value.set("cnt", cnt);
        context.write(key, value);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}