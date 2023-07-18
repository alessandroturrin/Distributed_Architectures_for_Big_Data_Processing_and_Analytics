package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.commons.io.output.NullWriter;
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
                    NullWritable> {// Output value type
    int count;
    
    protected void setup(Context context) {
    	count = 0;
    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		count++;
    		
    }
    
    protected void cleanup(Context context) {
    	System.out.println(count);
    }
}
