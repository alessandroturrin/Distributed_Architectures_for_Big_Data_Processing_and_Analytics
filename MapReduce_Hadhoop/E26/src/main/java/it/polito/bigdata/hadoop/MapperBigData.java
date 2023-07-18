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
                    NullWritable,         // Output key type
                    Text> {// Output value type
    
	protected HashMap<String, Integer> dictionary;
	
	protected void setup(Context context) throws IOException {
		String line;
		
		dictionary = new HashMap<String, Integer>();
		URI[] CachedFiles = context.getCacheFiles();

		BufferedReader fileStopWords = new BufferedReader(new FileReader(new File(CachedFiles[0].getPath())));

		while ((line = fileStopWords.readLine()) != null) {
			String s[] = line.split("\\t");
			dictionary.put(s[1], Integer.parseInt(s[0]));
		}
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		String words[] = value.toString().split(" ");
    		String msg = "";
    		
            for(String word: words)
            	msg = msg + dictionary.get(word) + " ";
            context.write(NullWritable.get(), new Text(msg));
    }
}
