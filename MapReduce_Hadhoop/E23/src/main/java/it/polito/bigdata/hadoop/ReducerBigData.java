package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
				NullWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
   
    String user;
    protected void setup(Context context) {
    	user = context.getConfiguration().get("user");
    }
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	HashSet<String> friendsSet = new HashSet<String>();
    	HashMap<String,String> usersMap = new HashMap<String,String>();
    	String msg = "";
    	
        for(Text value: values) {
        	String[] users = value.toString().split(",");
        	
        	if(!users[0].equals(user) && !users[1].equals(user))
        		usersMap.put(users[0], users[1]);
        	else {
        		if(users[0].equals(user))
        			friendsSet.add(users[1]);
        		else
        			friendsSet.add(users[0]);
        	}
        }
        
        for(Map.Entry<String, String> entry: usersMap.entrySet()) {
        	if(friendsSet.contains(entry.getKey()) && !friendsSet.contains(entry.getValue()))
        		msg = msg + entry.getValue() + " ";
        	if(!friendsSet.contains(entry.getKey()) && friendsSet.contains(entry.getValue()))
        		msg = msg + entry.getKey() + " ";
        }
        
        context.write(new Text(msg), NullWritable.get());
    }
}
