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
    	
    	HashMap<String, Double> items = new HashMap<>();
    	
    	int count = 0;
    	int sum = 0;
    	
		for(Text value: values) {
			String s[] = value.toString().split(",");
			count++;
			sum = sum + Integer.parseInt(s[1]);
			items.put(key+","+s[0], Double.parseDouble(s[1]));
		}
		
		double mean = (double)sum/(double)count;
		
		for(Map.Entry<String, Double> entry : items.entrySet()) {
			context.write(new Text(entry.getKey().split(",")[1]), 
					new DoubleWritable(entry.getValue()-mean));
		}
    }
}
