package zxy.LR;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record output;

    public void setup(TaskContext context) throws IOException {
        output = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        Double w0 =0.0;
        Double w1 =0.0;
        Double w2 =0.0;
        Double w3 =0.0;

        while (true) {
        	int batchNum = 0 ;
        	Double loss0 = 0.0;
        	Double loss1 = 0.0;
        	Double loss2 = 0.0;
        	Double loss3 = 0.0;

        	while(values.hasNext() && batchNum<100){
                Record val = values.next();
                Double x0 = val.getDouble(0);
                Double x1 = val.getDouble(1);
                Double x2 = val.getDouble(2);
                Double x3 = val.getDouble(3);
                Double label = (double) val.getBigint(4);

                batchNum ++;		
                double hx = 1/(1+Math.exp(-w0*x0-w1*x1-w2*x2-w3*x3));

                loss0 = loss0+(hx-label)*x0;
                loss1 = loss1+(hx-label)*x1;
                loss2 = loss2+(hx-label)*x2;
                loss3 = loss3+(hx-label)*x3;
        	}
        	
        	w0 = w0 - 0.01*loss0/batchNum-0.01*w0/batchNum;
        	w1 = w1 - 0.01*loss1/batchNum-0.01*w1/batchNum;
        	w2 = w2 - 0.01*loss2/batchNum-0.01*w2/batchNum;
        	w3 = w3 - 0.01*loss3/batchNum-0.01*w3/batchNum;	
        	
        	if (values.hasNext()==false) 
				break;
			
   
        }
        output.set(0, w0);
        output.set(1, w1);
        output.set(2, w2);
        output.set(3, w3);

        context.write(output);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
