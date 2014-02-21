package com.xyz.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserPurchaseTrendReducer extends Reducer<Text, Text, Text, Text> {

	private static final Log log = LogFactory
			.getLog(UserPurchaseTrendReducer.class);
	private static List<String> itemIncludeList = new ArrayList<String>();
	private static List<String> itemExcludeList = new ArrayList<String>();

	public void setup(Context context1) throws IOException,
			InterruptedException {
		String includeItems = context1.getConfiguration().get("INCLUDE_ITEM");
		String notIncludeItems = context1.getConfiguration().get(
				"NOT_INCLUDE_ITEM");

		addItemList(includeItems, itemIncludeList);
		addItemList(notIncludeItems, itemExcludeList);

	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		log.info("Enter method UserPurchaseTrendReducer.reduce()");
		boolean includeUser = true;
		boolean[] includeUsers = new boolean[itemIncludeList.size()];
		Arrays.fill(includeUsers, false);
		
		int counter = 0;
		String cxtValue = "";

		for (Text value : values) {
			String[] token = value.toString().split(",");
			cxtValue += value.toString() + ",";

			for (String itemExclude : itemExcludeList) {
				if (token[0].equals(itemExclude)) {
					includeUser = false;
					break;
				}
			}

			for (String itemInclude : itemIncludeList) {
				if (token[0].equals(itemInclude)) {
					includeUsers[counter] = true;
					counter++;
				}
			}
		}

		for (boolean present : includeUsers) {
			if (!present) {
				includeUser = false;
			}
		}

		if (includeUser) {

			context.write(key, new Text(cxtValue));
		}

		log.info("Exit method UserPurchaseTrendReducer.reduce()");

	}

	public void addItemList(String includeItems, List<String> strlist) {

		String[] items = includeItems.split(",");
		for (String item : items) {
			strlist.add(item);
		}
	}
}
