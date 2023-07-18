package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                NullWritable,           // Input key type
                IntWritable,    // Input value type
                NullWritable,           // Output key type
                NullWritable> {  // Output value type
    
    @Override
    protected void reduce(
    	NullWritable key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	int count = 0;
		/* Implement the reduce method */
    	for(IntWritable value: values)
    		count = count + value.get();
    	
    	System.out.println(count);
    }
}
