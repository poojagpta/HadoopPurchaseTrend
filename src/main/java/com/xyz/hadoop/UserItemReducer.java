package com.xyz.hadoop;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserItemReducer extends Reducer<UserItem, LongWritable, Text, Text> {

	private static final Log log = LogFactory.getLog(UserItemReducer.class);

	private Text itemList = new Text();
	private Text userIdDate = new Text();

	@Override
	public void reduce(UserItem key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		for (final LongWritable val : values) {	
			count+=val.get();
		}
		
		
		log.debug("######### " + count);
		log.debug("######### " + key.getUserId());
		String userIdProductStr = key.getUserId()+"\t"+key.getProduct(); 
		itemList.set(count+"");
		userIdDate.set(userIdProductStr);
		context.write(userIdDate, itemList);
	}

}
