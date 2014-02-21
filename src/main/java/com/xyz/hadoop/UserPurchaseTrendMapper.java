package com.xyz.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserPurchaseTrendMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private static final Log log = LogFactory
			.getLog(UserPurchaseTrendMapper.class);
	private static List<String> itemList = new ArrayList<String>();

	@Override
	public void setup(Context context1) {

		String includeItems = context1.getConfiguration().get("INCLUDE_ITEM");
		String notIncludeItems = context1.getConfiguration().get(
				"NOT_INCLUDE_ITEM");

		addItemList(includeItems);
		addItemList(notIncludeItems);

	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		log.info("Inside method UserPurchaseTrendMapper.map");
		String[] token = value.toString().split("\t");
		for (String item : itemList) {
			if (item.equalsIgnoreCase(token[1])) {

				context.write(new Text(token[0]), new Text(token[1] + ","
						+ token[2]));
			}
		}

		log.info("Exiting method UserPurchaseTrendMapper.map");
	}

	public void addItemList(String includeItems) {

		String[] items = includeItems.split(",");
		for (String item : items) {
			itemList.add(item);
		}
	}

}
