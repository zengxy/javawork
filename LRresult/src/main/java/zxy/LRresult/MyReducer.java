package zxy.LRresult;

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
        
    	int isRight = 0;
    	String itemMAX="";
    	String itemBought="";
    	double w0 = 0.232;
    	double w1 = 0.619;
    	double w2 = -0.489;
    	double w3 = -0.301;

		class ITEMCOUNT {
			String item = "";
			Double fx = Double.MIN_VALUE;
		}
		int TopK = 3;
		ITEMCOUNT[] topItems = new ITEMCOUNT[TopK];
		for (int i = 0; i < topItems.length; i++) {
			topItems[i] = new ITEMCOUNT();
		}
    	
    	
        while (values.hasNext()) {
            Record val = values.next();
            
            Double x0 = val.getDouble(1);
            Double x1 = val.getDouble(2);
            Double x2 = val.getDouble(3);
            Double x3 = val.getDouble(4);
            
            Double fxNow  = w0*x0+w1*x1+w2*x2+w3*x3;
            
    		int i = 0;
			while (i < TopK) {
				if (fxNow > topItems[i].fx) {

					for (int j = topItems.length - 2; j >= i; j--) {
						topItems[j + 1].fx = topItems[j].fx;
						topItems[j + 1].item = topItems[j].item;
					}

					topItems[i].fx = fxNow;
					topItems[i].item = val.getString("item_id");
					break;
				}
				i = i + 1;
			}

                     
            if(val.getBigint(5) == 1)
            	itemBought = val.getString(0);
   
        }
        
        
        output.set(0, key.get(0));
        output.set(1,key.get(1));

		for (int i = 0; i < topItems.length; i++) {
			if (topItems[i].item.equals(itemBought)) {
				for (int j = i; j < topItems.length; j++)
					output.set(j + 2, 1);
				context.write(output);
				return;
			} else
				output.set(i + 2, 0);
		}
		context.write(output);
	}
        
  
    public void cleanup(TaskContext arg0) throws IOException {

    }
}
