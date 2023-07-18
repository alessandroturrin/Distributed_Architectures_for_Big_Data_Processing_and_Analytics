package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    private HashMap<String,Integer> map;
    
	protected void setup(Context context) {
		map = new HashMap<>();
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
    	String words[] = value.toString().split("\\s+");
    	
    	for(String word: words) {
    		if(map.containsKey(word.toLowerCase()))
    			map.replace(word.toLowerCase(), map.get(word.toLowerCase())+1);
    		else
    			map.put(word.toLowerCase(), 1);
    	}
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	for(Map.Entry<String, Integer> entry: map.entrySet())
    		context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
    }
}
