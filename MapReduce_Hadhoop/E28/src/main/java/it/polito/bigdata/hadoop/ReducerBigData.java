package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;

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
    	
    	String question = null;
    	ArrayList<String> answers = new ArrayList<>();
    	
        for(Text value: values)
        	if(value.toString().startsWith("A"))
        		answers.add(value.toString());
        	else
        		question = key.toString() + "," + value.toString();
        
        for(String answer: answers)
        	context.write(new Text(question+","+answer), NullWritable.get());
    }
}
