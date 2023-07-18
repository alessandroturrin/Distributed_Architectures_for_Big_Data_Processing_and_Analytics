package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
	HashSet<String> stopWords;
	ArrayList<String> sentences;
	
	protected void setup(Context context) {
		stopWords = new HashSet<String>();
		sentences = new ArrayList<String>();
	}
	
    @Override
    
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        int occurrences = 0;

        // Iterate over the set of values and sum them 
        for (Text value : values)
            if(value.toString().contains(" "))
            	sentences.add(value.toString());
            else
            	stopWords.add(value.toString());
    }
    
    public void cleanup(Context context) throws IOException, InterruptedException {
    	for(String sentence: sentences) {
    		String msg = "";
    		String words[] = sentence.split("\\s+");
    		int count = 0;
    		for(String word: words) {
    			if(!sentences.contains(word)) {
    				if(count==0)
    					msg = msg + word;
    				else
    					msg = msg + " " + word;
    			}
    			count++;
    		}
    		context.write(new Text(msg), NullWritable.get());
    	}
    }
}
