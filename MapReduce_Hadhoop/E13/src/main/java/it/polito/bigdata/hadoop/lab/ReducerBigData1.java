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
                NullWritable,           // Input key type
                DateIncome,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
	
	
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<DateIncome> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	DateIncome topGlobal = new DateIncome(Integer.MIN_VALUE);
    	String date;
    	int income;
    	
		/* Implement the reduce method */
    	for(DateIncome value: values) {
    		date = value.getDate();
    		income = value.getIncome();
    		
    		if(income>topGlobal.getIncome()) 
    			topGlobal = new DateIncome(date, income);
    	}
    	
    	context.write(new Text(topGlobal.getDate()), new IntWritable(topGlobal.getIncome()));
    }
}
