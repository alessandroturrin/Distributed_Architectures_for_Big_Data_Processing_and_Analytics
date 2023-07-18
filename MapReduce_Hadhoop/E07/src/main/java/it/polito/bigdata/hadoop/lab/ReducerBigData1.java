package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	List<String> sIds = new ArrayList<String>();
		/* Implement the reduce method */
    	for(Text value: values)
    		sIds.add(value.toString());
    	
    	context.write(new Text(key), new Text(sIds.toString()));
    }
}
