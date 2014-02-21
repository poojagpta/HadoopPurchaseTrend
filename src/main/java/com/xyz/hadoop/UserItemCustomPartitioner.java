package com.xyz.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class UserItemCustomPartitioner extends Partitioner<UserItem, LongWritable> {

	HashPartitioner<Text, LongWritable> hashPartitioner = new HashPartitioner<Text, LongWritable>();
	Text newKey = new Text();

	@Override
	public int getPartition(UserItem key, LongWritable value, int numReduceTasks) {

		try {
			// Execute the default partitioner over the first part of the key
			newKey.set(key.getUserId());
			return hashPartitioner.getPartition(newKey, value, numReduceTasks);
		} catch (Exception e) {
			e.printStackTrace();
			return (int) (Math.random() * numReduceTasks); // this would return
															// a random value in
															// the range
															// [0,numReduceTasks)
		}
	}

}
