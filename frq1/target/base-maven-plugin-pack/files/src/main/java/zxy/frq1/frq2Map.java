package zxy.frq1;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

public class frq2Map extends MapperBase {
	private Record key;
	private Record value;

	@Override
	public void setup(TaskContext context) throws IOException {
		key=context.createMapOutputKeyRecord();
		value=context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context)
			throws IOException {
		key.set("session",record.getBigint(0));
		key.set("user_id",record.getString(1));
		value.set("item_id",record.getBigint(2).toString());
		context.write(key,value);
	}

}
