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
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
    	String s[] = value.toString().split(",");
    	
    	if(s.length>2)
    		for(int i=1;i<s.length;i++)
    			for(int j=1;j<s.length;j++)
    				if(s[i].compareTo(s[j])<0)
    					context.write(new Text(s[i] + "," + s[j]), new IntWritable(1));
    }
}
