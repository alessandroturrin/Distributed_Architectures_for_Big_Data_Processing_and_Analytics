package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import it.polito.bigdata.hadoop.lab.DriverBigData;
import it.polito.bigdata.hadoop.lab.MapperBigData1;

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

		Path inputPath;
		Path outputDir;
		int numberOfReducersJob1;

		inputPath = new Path(args[0]);
		outputDir = new Path(args[1]);

		// Set the path of the input file/folder for this first job
		FileInputFormat.addInputPath(job, inputPath);
		
		FileOutputFormat.setOutputPath(job, outputDir);
		// Set the path of the output folder for this job
		MultipleOutputs.addNamedOutput(job, "hightemp", TextOutputFormat.class, NullWritable.class, Writable.class);
		MultipleOutputs.addNamedOutput(job, "normaltemp", TextOutputFormat.class, NullWritable.class, Writable.class);

		// Specify the class of the Driver for this job
		job.setJarByClass(DriverBigData.class);

		// Set job input format
		job.setInputFormatClass(TextInputFormat.class);

		// Set map class
		job.setMapperClass(MapperBigData1.class);

		// Set map output key and value classes
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

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
