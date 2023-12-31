package it.polito.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * MapReduce program
 */
public class DriverBigData extends Configured 
implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Path inputPath1;
    Path inputPath2;
    Path outputDir;
	int exitCode;  
	
    // Folder containing the input data
    inputPath1 = new Path(args[0]);
    inputPath2 = new Path(args[1]);
    // Output folder
    outputDir = new Path(args[2]);
    
    Configuration conf = this.getConf();

    // Define a new job
    Job job = Job.getInstance(conf); 

    // Assign a name to the job
    job.setJobName("Basic MapReduce Project - WordCount example");
    
    // Specify the class of the Driver for this job
    job.setJarByClass(DriverBigData.class);
    // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
    MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, MapperBigData.class);
    MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, MapperBigData.class);
    // Set path of the output folder for this job
    FileOutputFormat.setOutputPath(job, outputDir);
    
    // Set map output key and value classes
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setReducerClass(ReducerBigData.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    // Set job output format
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.setNumReduceTasks(1);
    
    
    // Execute the job and wait for completion
    if (job.waitForCompletion(true)==true)
    	exitCode=0;
    else
    	exitCode=1;
    	
    return exitCode;
  }
  

  /** Main of the driver
   */
  
  public static void main(String args[]) throws Exception {
	// Exploit the ToolRunner class to "configure" and run the Hadoop application
    int res = ToolRunner.run(new Configuration(), 
    		new DriverBigData(), args);

    System.exit(res);
  }
}