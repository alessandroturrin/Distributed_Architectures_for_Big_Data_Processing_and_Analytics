package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	int count = 0;
    	float tot = 0;
    	
		/* Implement the reduce method */
    	for(FloatWritable value: values) {
    		count++;
    		tot = tot + value.get();
    	}
    		
    	context.write(new Text(key), new FloatWritable(tot/(float)count));
    }
}
