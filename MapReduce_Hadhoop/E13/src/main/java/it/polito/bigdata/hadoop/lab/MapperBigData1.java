package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

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
                    DateIncome> {// Output value type
    
	DateIncome top1; 
	
	protected void setup(Context context) {
		top1 = new DateIncome(Integer.MIN_VALUE);
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		String date = value.toString().split("\\t")[0];
    		int income = Integer.parseInt(value.toString().split("\\t")[1]);
    		
    		if(income>top1.getIncome())
    			top1 = new DateIncome(date, income);
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	context.write(NullWritable.get(), top1);
    }
}
