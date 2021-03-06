package com.xyz.AlgorithmExample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EulerTour {

	static enum EulerCounters {

		numberOfIterations, // to determine if the iteration has to proceed
		moreWhiteNode, // to determine if the graph is connected
		isNotEvenDegree
		// to determine if the graph is of even degree
	}

	/**
	 * 
	 * Description : Mapper class that implements the map part of checking for connectivity of a graph. It extends the SearchMapper class and calls the super class' map method..
	 * Two nodes are said to be connected if there is a path between the two nodes in the graph.
	 *       
	 * Reference : http://www.johnandcailin.com/blog/cailin/breadth-first-graph-search-using-iterative-map-reduce-algorithm
	 * 
	 *      
	 */
	public static class ConnectivityMapper extends SearchMapper {

		
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
		
			Node inNode = new Node(value.toString());
			super.map(key, value, context, inNode);

		}
	}
	
	/**
	 * 	  
	 * Description : Reducer class that implements the reduce part of checking for connectivity of a graph.  It extends the SearchReducer class.
	 * It calls the super class' reduce method and increments the counter if the color of the returned node is WHITE indicating that the graph is not connected.
	 * 
	 * Input format <key, value> : <nodeID,  list_of_adjacent_nodes|distance_from_the_source|color|parent_node>
	 * 
	 * Output format <key, value> : <nodeID, (updated) list_of_adjacent_nodes|distance_from_the_source|color|parent_node>
	 * 
	 * Reference : http://www.johnandcailin.com/blog/cailin/breadth-first-graph-search-using-iterative-map-reduce-algorithm
	 * 
	 *         
	 */

	// the type parameters are the input keys type, the input values type, the
	// output keys type, the output values type

	public static class ConnectivityReducer extends SearchReducer{


		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			
			//create a new out node and set its values
			Node outNode = new Node();
			//call the reduce method of SearchReducer class 
			outNode = super.reduce(key, values, context, outNode);
			
			// if the color of the node is gray, the execution has to continue
			if (outNode.getColor()== Node.Color.GRAY) {
				context.getCounter(EulerCounters.numberOfIterations).increment(
						1L);

			}
			
			if ( outNode.getColor() == Node.Color.WHITE) {

				context.getCounter(EulerCounters.moreWhiteNode).increment(1L);

			}

		}
	}

	/**
	 * Description : Mapper class that prepares the data suitable for the DegreeReducer that checks if all the nodes are of even degree.
	 * 
	 * Input format <key, value>  : nodeID<tab>list_of_adjacent_nodes|distance_from_the_source|color|parent
	 * 
	 * Output format <key, value> : < nodeID, (updated) list_of_adjacent_nodes|distance_from_the_source|color|parent>
	 *  
	 *         
	 */

	// the type parameters are the input keys type, the input values type, the
	// output keys type, the output values type
	public static class DegreeMapper extends Mapper<Object, Text, Text, Text> {

		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Node node = new Node(value.toString());
			context.write(new Text(node.getId()), node.getNodeInfo());

		}
	}
	
	/**
	 *  Description : Reducer class that checks if all the nodes are of even degree and increments the counter value even if one node is not of
	 *  even degree. The process of checking does not terminate when a node of odd degree is found. The process continues until the degree of all the nodes are checked.
	 *   
	 *  Input format <key, value> : <nodeID,  list_of_adjacent_nodes|distance_from_the_source|color|parent>
	 *  
	 *  Output format <key, value> : <nodeID, (updated) list_of_adjacent_nodes|distance_from_the_source|color|parent>
	 *   
	 *         
	 */


	public static class DegreeReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {

				Node inNode = new Node(key.toString() + "\t" + value.toString());
				int degree = inNode.getEdges().size(); // get the degree of the node

				if (degree % 2 != 0) { // if there is at least one node whose
					// degree is odd, set the variable
					// isEvenDegree to false and break

					context.getCounter(EulerCounters.isNotEvenDegree)
							.increment(1L);
					
				}

				context.write(key, value);
			}

		}
	}


	// the driver to execute two jobs and invoke the map/reduce functions

	public int run(String[] args) throws Exception {

		long isNotConnected = 0, isNotEvenDegree = 0;

		isNotConnected = connectivityJob(args[0], args[1]);
		isNotEvenDegree = degreeJob(args[0], args[2]);

		if (isNotConnected == 0 && isNotEvenDegree > 0)
			System.out
					.println("connected but not all vertices are even degree ");
		else if (isNotConnected > 0 && isNotEvenDegree == 0)
			System.out
					.println("not connected but all vertices are of even degree");
		else if (isNotConnected > 0 && isNotEvenDegree > 0)
			System.out
					.println("not connected and not all vertices are of even degree");
		else if (isNotConnected == 0 && isNotEvenDegree == 0)
			System.out
					.println("Connected and all vertices are of even degree. Euler tour is present");

		return 0;

	}
	
	// executing the job to determine if the graph is connected
	private long connectivityJob(String inputPath, String outputPath)
			throws Exception {

		int iterationCount = 0; // counter to set the ordinal number of the
		// intermediate outputs

		Job job = null;
		long terminationValue = 1;

		// while there are more gray nodes to process
		while ( terminationValue > 0) {

			job = new Job(new Configuration(),"New Job");
			job.setJarByClass(EulerTour.class);
			job.setMapperClass(ConnectivityMapper.class);
			job.setReducerClass(ConnectivityReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			String input;
			if (iterationCount == 0) // for the first iteration , the input will
				// be the input_graph
				input = inputPath;
			else
				// for the remaining iterations, the input will be the output of
				// the previous iteration
				input = outputPath + iterationCount;

			String output = outputPath + (iterationCount + 1); // setting the output file

			FileInputFormat.setInputPaths(job, new Path(input)); // setting the input files for the job
			FileOutputFormat.setOutputPath(job, new Path(output)); // setting the output files for the job

			iterationCount++;

			job.waitForCompletion(true); // wait for the job to complete

			Counters jobCntrs = job.getCounters();
			terminationValue = jobCntrs.findCounter(
					EulerCounters.numberOfIterations).getValue();

		}

		Counters jobCntrs = job.getCounters();
		long whiteNodeCnt = jobCntrs.findCounter(EulerCounters.moreWhiteNode)
				.getValue();
						
		if(terminationValue == 0 && whiteNodeCnt > 0){
			return whiteNodeCnt;
		}
		else{	
			return 0;
		}
		
	}
	
	// executing the job to determine if every vertex is of even degree
	private long degreeJob(String inputPath, String outputPath)
			throws Exception {

		Job degree_job = new Job();

		degree_job.setMapperClass(DegreeMapper.class);
		degree_job.setReducerClass(DegreeReducer.class);
		degree_job.setOutputKeyClass(Text.class);
		degree_job.setOutputValueClass(Text.class);
		degree_job.setJarByClass(EulerTour.class);
		
		FileInputFormat.setInputPaths(degree_job, new Path(inputPath)); // setting the input files for the job
		FileOutputFormat.setOutputPath(degree_job, new Path(outputPath)); // setting the output files for the job

		degree_job.waitForCompletion(true);

		Counters jobCntrs = degree_job.getCounters();
		long isEvenDegree = jobCntrs.findCounter(EulerCounters.isNotEvenDegree)
				.getValue();
		return isEvenDegree;

	}

	
	public static void main(String[] args) throws Exception {

		
		if (args.length != 3) {
			System.err
					.println("Usage: Euler tour <in> <output_search> <output_degree>");
			System.exit(2);
		}
		
		System.exit(new EulerTour().run(args));
	}

}