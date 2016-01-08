package zxy.segment;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

public class segmentMap extends MapperBase {
	private Record key;
	private Record value;
	@Override
	public void setup(TaskContext context) throws IOException {
		key=context.createMapOutputKeyRecord();
		value=context.createMapOutputValueRecord();
		System.out.println("TaskID"+context.getTaskID().toString());
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context)
			throws IOException {
				key.set("user_id",record.getString(0));
				value.set("vtime",record.getString(4));
				value.set("item_id",record.getBigint(1));
				value.set("category",record.getString(2));
				value.set("action",record.getString(3));
				context.write(key,value);
	}
}
