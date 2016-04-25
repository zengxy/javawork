package zxy.GraphTest;

import java.io.IOException;

import com.aliyun.odps.graph.GraphLoader;
import com.aliyun.odps.graph.MutationContext;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.WritableRecord;

public class MyGraphSSSPVertexReader extends
		GraphLoader<LongWritable, LongWritable, LongWritable, LongWritable> {

	@Override
	public void load(
			LongWritable recordNum,
			WritableRecord record,
			MutationContext<LongWritable, LongWritable, LongWritable, LongWritable> context)
			throws IOException {
		MyGraphSSSPVertex vertex = new MyGraphSSSPVertex();
		vertex.setId((LongWritable) record.get(0));
		String[] edges = record.get(1).toString().split("\\t");
		for (int i = 0; i < edges.length; i++) {
			String[] ss = edges[i].split(":");
			vertex.addEdge(new LongWritable(Long.parseLong(ss[0])),
					new LongWritable(Long.parseLong(ss[1])));
		}

		context.addVertexRequest(vertex);
	}
}
