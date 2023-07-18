package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	HashSet<String> userLikes = new HashSet<>();
    	String data = null;
    	
        for(Text value: values)
        	if(!value.toString().contains(","))
        		userLikes.add(value.toString());
        	else
        		data = value.toString();
        
        if(userLikes.contains("Commedia") && userLikes.contains("Adventure"))
        	context.write(new Text(data), NullWritable.get());
    }
}
