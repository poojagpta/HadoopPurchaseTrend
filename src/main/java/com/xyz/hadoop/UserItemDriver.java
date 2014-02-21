package com.xyz.hadoop;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.xyz.thread.JobRunner;

public class UserItemDriver {

	private static final Log log = LogFactory.getLog(UserItemDriver.class);

	public static void main(String[] args) throws Exception {

		final Configuration conf = new Configuration();
		

		String envt = null;

		if (args.length > 0) {
			envt = args[0];
		} else {
			envt = "dev";
		}

		Properties prop = new Properties();

		try {
			envt = "dev";
			// load a properties file from class path, inside static method
			prop.load(UserItemDriver.class.getClassLoader()
					.getResourceAsStream("config-" + envt + ".properties"));

		} catch (IOException ex) {
			ex.printStackTrace();
			System.exit(1);
		}

		log.debug("Message : " + prop.getProperty("message"));

		log.debug("Conf: " + conf);

		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		// First Job
		final Job job = new Job(conf,
				"Count the items from bugs bunny data on hdfs in \"input\" path.");

		job.setJarByClass(UserItemDriver.class);
		job.setMapperClass(UserItemMapper.class);

		// job.setCombinerClass(UserItemReducer.class);
		job.setReducerClass(UserItemReducer.class);

		job.setMapOutputKeyClass(UserItem.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// FileInputFormat.setInputPaths(job, new Path("inputUserItem"));
		// FileOutputFormat.setOutputPath(job, new Path("output"));

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Deleting the output file if already exist
		Path outputPath = new Path(args[1]);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			System.out.println("Deleting output path before proceeding.");
			fs.delete(outputPath, true);
		}

		ControlledJob jobCon1 = new ControlledJob(conf);
		jobCon1.setJob(job);

		// Second job
		conf.set("INCLUDE_ITEM", args[2]);
		conf.set("NOT_INCLUDE_ITEM", args[3]);

		final Job job2 = new Job(conf,
				"Count the items from bugs bunny data on hdfs in \"input\" path.");

		job2.setJarByClass(UserItemDriver.class);
		job2.setMapperClass(UserPurchaseTrendMapper.class);
		job2.setReducerClass(UserPurchaseTrendReducer.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		//job2.setNumReduceTasks(0);

		// FileInputFormat.setInputPaths(job, new Path("inputUserItem"));
		// FileOutputFormat.setOutputPath(job, new Path("output"));

		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "1"));

		// Deleting the output file if already exist
		outputPath = new Path(args[1] + "1");
		fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			System.out.println("Deleting output path before proceeding.");
			fs.delete(outputPath, true);
		}

		ControlledJob jobCon2 = new ControlledJob(conf);
		jobCon2.setJob(job2);

		JobControl jobctrl = new JobControl("jobctrl");
		jobctrl.addJob(jobCon1);
		jobctrl.addJob(jobCon2);
		jobCon2.addDependingJob(jobCon1);
		handleRun(jobctrl);
		jobctrl.stop();
		// System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static void handleRun(JobControl control)
			throws InterruptedException {
		JobRunner runner = new JobRunner(control);
		Thread t = new Thread(runner);
		t.start();

		while (!control.allFinished()) {
			System.out.println("Still running...");
			Thread.sleep(5000);
		}
	}

}
