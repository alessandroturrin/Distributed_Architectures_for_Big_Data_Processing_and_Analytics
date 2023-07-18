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
    
	double threshold;
	
	protected void setup(Context context) {
		threshold = Double.parseDouble(context.getConfiguration().get("threshold").toString());
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		String sId = value.toString().split(",")[0];
    		double val = Double.parseDouble(value.toString().split(",")[1].split("\\t")[1]);
    		
    		if(val>threshold)
    			context.write(new Text(sId), new IntWritable(1));
    		
    }
}
