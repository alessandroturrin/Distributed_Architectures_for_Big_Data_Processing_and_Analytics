package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                DoubleWritable,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<DoubleWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	double max = Double.MIN_VALUE;
    	double min = Double.MAX_VALUE;
    	
		/* Implement the reduce method */
    	for(DoubleWritable value: values) {
    		if(value.get()>max)
    			max = value.get();
    		if(value.get()<min)
    			min = value.get();
    	}
    	
    	context.write(new Text(key), new Text("max="+max+"_min="+min));
    }
}
