package zxy.GraphTest;

import java.io.IOException;

import com.aliyun.odps.graph.Combiner;
import com.aliyun.odps.io.LongWritable;

public class MyGraphSSSPCombiner extends Combiner<LongWritable, LongWritable> {

	@Override
	public void combine(LongWritable vertexId, LongWritable combinedMessage,
			LongWritable messageToCombine) throws IOException {
		if (combinedMessage.get() > messageToCombine.get()) {
			combinedMessage.set(messageToCombine.get());
		}
	}

}
