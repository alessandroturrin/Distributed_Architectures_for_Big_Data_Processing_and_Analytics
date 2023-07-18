package it.polito.bigdata.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

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
                    Text,         // Output key type
                    NullWritable> {// Output value type
    
	HashMap<String, String> dictionary;
	
	protected void setup(Context context) throws IOException {
		dictionary = new HashMap<>();
		String line;
		
		URI[] CachedFiles = context.getCacheFiles();
		BufferedReader file = new BufferedReader(new FileReader(new File(CachedFiles[0].getPath())));
		
		while((line=file.readLine())!=null) {
			String s[] = line.split(" ");
			dictionary.put(s[0]+","+s[2], s[4]);
		}
		
		file.close();
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    	String elements[] = value.toString().split(",");
    	String check = "Gender="+elements[3]+","+"YearOfBirth="+elements[4];
    	String category;
    	
    	category = dictionary.get(check);
    	
    	if(category!=null)
    		context.write(new Text(value.toString() + "," + category), NullWritable.get());
    	else
    		context.write(new Text(value.toString() + ",Unknown"), NullWritable.get());
    }
}
