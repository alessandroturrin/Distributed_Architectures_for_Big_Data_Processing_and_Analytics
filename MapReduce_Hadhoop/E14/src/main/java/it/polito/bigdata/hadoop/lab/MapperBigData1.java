package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
    
	private HashSet<String> singleWords;
	
	protected void setup(Context context) {
		singleWords = new HashSet<String>();
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
    	String words[] = value.toString().split("\\s+");
    	
    	for(String word: words)
    		singleWords.add(word.toLowerCase());
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	context.write(NullWritable.get(), new Text(singleWords.toString()));
    }
}
