package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                NullWritable,           // Output key type
                WordCountWritable> {  // Output value type
    
	TopKVector<WordCountWritable> topKWords;
	int k = 100;
	protected void setup(Context context) {
		topKWords = new TopKVector<WordCountWritable>(k);
	}
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	int count = 0;
    	
		for(IntWritable value: values)
			count = count + value.get();
		
		WordCountWritable w = new WordCountWritable(new String(key.toString()), count);
		
		topKWords.updateWithNewElement(w);
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
		// emit the local top-k list
		for (WordCountWritable p : topKWords.getLocalTopK()) {
			context.write(NullWritable.get(), new WordCountWritable(p));
		}
	}
}
