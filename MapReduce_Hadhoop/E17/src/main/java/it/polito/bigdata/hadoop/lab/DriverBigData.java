package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import it.polito.bigdata.hadoop.lab.DriverBigData;
import it.polito.bigdata.hadoop.lab.MapperBigData1;
import it.polito.bigdata.hadoop.lab.MapperBigData2;
import it.polito.bigdata.hadoop.lab.ReducerBigData1;

/**
 * MapReduce program
 */
public class DriverBigData extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		int exitCode;

		Configuration conf = this.getConf();

		// Define a new job
		Job job = Job.getInstance(conf);

		// Assign a name to the job
		job.setJobName("Lab#4 - Ex.1 - step 1");

		/*
		 * ********************************************************* 
		 * Fill out the missing parts/update the content of this method
		 ************************************************************
		*/
		int nReducers;
		Path inputPath1;
		Path inputPath2;
		Path outputDir;
		
		// Parse the parameters for the set up of the first job
		nReducers = Integer.parseInt(args[0]);
		inputPath1 = new Path(args[1]);
		inputPath2 = new Path(args[2]);
		outputDir = new Path(args[3]);
		
		MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, MapperBigData1.class);
		MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, MapperBigData2.class);
		// Set the path of the input file/folder for this first job
		job.setJarByClass(DriverBigData.class);
		// Set the path of the output folder for this job
		FileOutputFormat.setOutputPath(job, outputDir);

		// Set job output format
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set map class
		job.setMapperClass(MapperBigData1.class);

		// Set map output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		// Set reduce class
		job.setReducerClass(ReducerBigData1.class);

		// Set reduce output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		// Set number of reducers
		job.setNumReduceTasks(nReducers);

		// Execute the first job and wait for completion
		if (job.waitForCompletion(true) == true) {
			exitCode = 0;
		} else
			exitCode = 1;

		return exitCode;

	}

	/**
	 * Main of the driver
	 */

	public static void main(String args[]) throws Exception {
		// Exploit the ToolRunner class to "configure" and run the Hadoop
		// application
		int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

		System.exit(res);
	}
}
