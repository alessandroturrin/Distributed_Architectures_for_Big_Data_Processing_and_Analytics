package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

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
                    Text> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
    	String values[] = value.toString().split(",");
    	
    	if (values[0].compareTo("Id") != 0) {
	    	String productId = values[1];
	    	String userId = values[2];
	    	int score = Integer.parseInt(values[6]);
	    	
	    	context.write(new Text(userId), new Text(productId+","+score));
    	}
    }
}
