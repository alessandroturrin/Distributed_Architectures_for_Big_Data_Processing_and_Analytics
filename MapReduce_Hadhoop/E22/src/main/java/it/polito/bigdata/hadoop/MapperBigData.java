package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
    
	String user;
	protected void setup(Context context) {
		user = context.getConfiguration().get("user");
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String users[] = value.toString().split(",");
            
            if(users[0].equals(user))
            	context.write(NullWritable.get(), new Text(users[1]));
            if(users[1].equals(user))
            	context.write(NullWritable.get(), new Text(users[0]));
    }
}
