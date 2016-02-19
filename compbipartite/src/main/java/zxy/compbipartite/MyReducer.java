package zxy.compbipartite;

import apsara.odps.lot.LanguageSinkProtos.LanguageSink.Output;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
//以brand作为comp的版本
public class MyReducer implements Reducer {
	private Record output;
	//不同的brand的最小数目
	private final int MIN_DIFFERENT_BRAND=1;
	//可比较SESSION的最小长度(此处session为session中同样类别产品组成的子session)
	private final int MIN_SESSION_LEN=5;
	
	
	
	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		// List<Object[]> rList=new ArrayList<Object[]>();
		Map<String, Integer> brand_count = new HashMap<String, Integer>();
		int sessionLen = 0; 
		int numOfDifferentItem = 0;
		while (values.hasNext()) {
			Record val = values.next();
			String brand = val.getString("brand_id");
			sessionLen++;
			if (brand_count.keySet().contains(brand))
				brand_count.put(brand, brand_count.get(brand) + 1);
			else {
				brand_count.put(brand, 1);
				numOfDifferentItem++;
			}
		}

		if(numOfDifferentItem < MIN_DIFFERENT_BRAND || sessionLen < MIN_SESSION_LEN )
			return;;
		
		
		for (String brand : brand_count.keySet()) {
			output.set(0,key.getString("user_id")+"_"+key.getBigint("session").toString()+"_"+key.getString("category"));
			output.set(1,sessionLen);
			output.set(2,brand);
			output.set(3,brand_count.get(brand));
			context.write(output);
		}
	}

	public void cleanup(TaskContext arg0) throws IOException {

	}
}
