package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
                Text,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	Map<String, Integer> months = new HashMap<>();
    	int tot = 0;
    	int count = 0;
    	
		/* Implement the reduce method */
    	for(Text value: values) {
    		String month = value.toString().split(",")[0];
    		int income = Integer.parseInt(value.toString().split(",")[1]);
    		
    		if(!months.containsKey(month))
    			months.put(month, income);
    		else
    			months.replace(month, months.get(month)+income);
    		
    		tot = tot + income;
    		count++;
    	}
    	
    	for(Map.Entry<String, Integer> entry: months.entrySet())
    		context.write(new Text(key+"-"+entry.getKey()), new DoubleWritable((double)entry.getValue()));
    	context.write(new Text(key), new DoubleWritable((double)tot/(double)count));
    }
}
