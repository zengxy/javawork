package zxy.GraphTest;

import java.io.IOException;

import com.aliyun.odps.graph.ComputeContext;
import com.aliyun.odps.graph.Edge;
import com.aliyun.odps.graph.Vertex;
import com.aliyun.odps.graph.WorkerContext;
import com.aliyun.odps.io.LongWritable;

public class MyGraphSSSPVertex extends
		Vertex<LongWritable, LongWritable, LongWritable, LongWritable> {

	public MyGraphSSSPVertex() {
		this.setValue(new LongWritable(Long.MAX_VALUE));
	}

	@Override
	public void compute(
			ComputeContext<LongWritable, LongWritable, LongWritable, LongWritable> context,
			Iterable<LongWritable> messages) throws IOException {
		long minDist = getId().get() == 1 ? 0 : Integer.MAX_VALUE;

		for (LongWritable msg : messages) {
			if (msg.get() < minDist) {
				minDist = msg.get();
			}
		}

		if (minDist < this.getValue().get()) {
			this.setValue(new LongWritable(minDist));
			if (hasEdges()) {
				for (Edge<LongWritable, LongWritable> e : this.getEdges()) {
					context.sendMessage(e.getDestVertexId(), new LongWritable(
							minDist + e.getValue().get()));
				}
			}
		} else {
			voteToHalt();
		}
	}

	@Override
	public void cleanup(
			WorkerContext<LongWritable, LongWritable, LongWritable, LongWritable> context)
			throws IOException {
		context.write(getId(), getValue());
	}
}