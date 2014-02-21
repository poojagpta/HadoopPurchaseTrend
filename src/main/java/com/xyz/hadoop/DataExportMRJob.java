package com.xyz.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class DataExportMRJob {

	/**
	 * @param args
	 */
	// TODO Auto-generated method stub
	// comments, quality etc.
	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();

		//conf.set("fs.file.impl",
			//	"com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");

		DBConfiguration.configureDB(conf,
				"com.mysql.jdbc.Driver",
				"jdbc:mysql://localhost:3306", "root",
				"root");

		Job job = new Job(conf);

		job.setJarByClass(DataExportMRJob.class);
		job.setMapperClass(DataMapper.class);
		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(DBOutputWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(FileInputFormat.class);

		job.setOutputFormatClass(DBOutputFormat.class);

		FileInputFormat
				.setInputPaths(job, new Path(args[0]));

		DBOutputFormat.setOutput(job, "ext_faa",
				new String[] { "name", "count" });

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
