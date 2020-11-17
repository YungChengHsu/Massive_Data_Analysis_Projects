package org.apache.hadoop.examples;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank {
	private static int numberOfNodesInGraph = 10876;
	private static double normalizationValue = 0.0;

	public static class Map extends Mapper<Object, Text, IntWritable, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String input_value = value.toString();
			String[] parts = input_value.split("\t");
			int adjListSize = parts.length - 3;
			double p = Double.parseDouble(parts[1]) / Double.valueOf(adjListSize);
			context.write(new IntWritable(Integer.parseInt(parts[0])), new Text(input_value));
			for(int i = 3; i < parts.length; i++) {
				context.write(new IntWritable(Integer.parseInt(parts[i])), new Text(parts[i] + "\t" + Double.toString(p)));
			}
		}
	}

	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double pagerank = 0.0;
			String nodeId = "";
			String structureOutput = "";
			for(Text value: values) {
				String input_value = value.toString();
				String[] parts = input_value.split("\t");
				nodeId = parts[0];
				if(parts.length > 2) {
					for(int i = 3; i < parts.length; i++) {
						structureOutput = structureOutput + "\t" + parts[i];
					}
				} else {
					pagerank += Double.parseDouble(parts[1]);
				}
			}

			pagerank = 0.8 * pagerank + (0.2 / Double.valueOf(numberOfNodesInGraph));
			normalizationValue += pagerank;
			String outValue = String.valueOf(pagerank) + "\t" + String.valueOf(pagerank) + structureOutput;
			context.write(key, new Text(outValue));
		}
	}

	public static class NormalizationMapper extends Mapper<Object, Text, IntWritable, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String input_value = value.toString();
			String[] parts = input_value.split("\t");
			int adjListSize = parts.length - 3;
			double pagerank = Double.parseDouble(parts[1]) + ((1.0 - normalizationValue) / Double.valueOf(numberOfNodesInGraph));
			String outValue = "";
			for(int i = 3; i < parts.length; i++) {
				outValue = outValue + "\t" + parts[i];
			}
			context.write(new IntWritable(Integer.parseInt(parts[0])), new Text(parts[0] + "\t" + Double.toString(pagerank) + "\t" + Double.toString(pagerank) + outValue));
		}
	}

	public static class NormalizationReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for(Text value: values) {
				String[] parts = value.toString().split("\t");
				String outValue = parts[1];
				for(int i = 2; i < parts.length; i++) {
					outValue = outValue + "\t" + parts[i];
				}
				context.write(key, new Text(outValue));
			}

		}
	}

	public static class TokenizerMapper
        extends Mapper<Object, Text, Text, Text>{

        public static final char fieldSeparator = '\t';
	    private Text outKey = new Text();
	    private Text outValue = new Text();

	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	        String[] parts = value.toString().split("\t");
	        outKey.set(parts[0]);
	        outValue.set(parts[1]);
		    context.write(outKey, outValue);
	    }
	}

	public static class IntSumReducer
	        extends Reducer<Text, Text, Text, Text> {
	    public Text result = new Text();

	    public void reduce(Text key, Iterable<Text> values,
	                        Context context
	                        ) throws IOException, InterruptedException {
	        String sum = "0.000092";
	        for (Text val : values) {
	        	sum = sum + "\t" + val.toString();
	        }
	        result.set(sum);
	        context.write(key, result);
	    }
	}

	public static class NodeForSorting {
		public String nodeId;
		public double pagerank;
	}

    public static class PageRankComparator implements Comparator<NodeForSorting> {

        @Override
        public int compare(NodeForSorting v1, NodeForSorting v2) {
            if(v1.pagerank < v2.pagerank) return 1;

            return -1;
        }
    }

	public static class TopKMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\t");
            context.write(new Text("sorting"), new Text(parts[0] + "\t" + parts[1]));
        }
    }

    public static class TopKReducer
            extends Reducer<Text, Text, Text, Text> {

        private PriorityQueue<NodeForSorting> TopKRecords;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Comparator<NodeForSorting> comparator = new PageRankComparator();
            TopKRecords = new PriorityQueue<NodeForSorting>(comparator);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
            	String[] parts = value.toString().split("\t");
            	NodeForSorting node = new NodeForSorting();
            	node.nodeId = parts[0];
            	node.pagerank = Double.parseDouble(parts[1]);
            	TopKRecords.add(node);
            }

            for(int i = 1; i <= 10; i++) {
            	NodeForSorting nfs = TopKRecords.poll();
            	double pagerankToReducer = nfs.pagerank;
            	context.write(new Text(String.valueOf(i)), new Text(nfs.nodeId + "\t" + Double.toString(pagerankToReducer)));
            }
        }
    }

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Configuration preInputconf = new Configuration();
		Job jobProcessInput = new Job(preInputconf, "Process Input");
	    jobProcessInput.setJarByClass(PageRank.class);
	    jobProcessInput.setMapperClass(TokenizerMapper.class);
	    jobProcessInput.setCombinerClass(IntSumReducer.class);
	    jobProcessInput.setReducerClass(IntSumReducer.class);
	    jobProcessInput.setOutputKeyClass(Text.class);
	    jobProcessInput.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(jobProcessInput, inputPath);
	    FileOutputFormat.setOutputPath(jobProcessInput, outputPath);
		jobProcessInput.waitForCompletion(true);

		inputPath = new Path(args[1] + "/part-r-00000");

		for(int i = 0; i < 20; i++) {
			normalizationValue = 0.0;
			outputPath = new Path(args[1] + "_pr_" + Integer.toString(i + 1));	
			
			Job job = Job.getInstance(new Configuration());
			job.setJarByClass(PageRank.class);
			job.setJobName("Page Rank");
			
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(job, inputPath);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);

			job.waitForCompletion(true);

			inputPath = new Path(args[1] + "_pr_" + Integer.toString(i + 1) + "/part-r-00000");
			outputPath = new Path(args[1] + "_prN_" + Integer.toString(i + 1));

			Configuration normalizationConf = new Configuration();
			Job norJob = Job.getInstance(normalizationConf);

			norJob.setJarByClass(PageRank.class);
			norJob.setJobName("Page Rank Normalization");

			norJob.setMapperClass(NormalizationMapper.class);
			norJob.setReducerClass(NormalizationReducer.class);

			norJob.setInputFormatClass(TextInputFormat.class);
			norJob.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(norJob, inputPath);
			FileOutputFormat.setOutputPath(norJob, outputPath);

			norJob.setOutputKeyClass(IntWritable.class);
			norJob.setOutputValueClass(Text.class);

			norJob.waitForCompletion(true);

			inputPath = new Path(args[1] + "_prN_" + Integer.toString(i + 1) + "/part-r-00000");
		}

		inputPath = new Path(args[1] + "_prN_20/part-r-00000");
		outputPath = new Path(args[1] + "_prSorted");
		Job jobSorting = Job.getInstance(new Configuration());
		jobSorting.setJarByClass(PageRank.class);
		jobSorting.setJobName("Page Rank Sorting");
			
		jobSorting.setMapperClass(TopKMapper.class);
		jobSorting.setReducerClass(TopKReducer.class);
			
		jobSorting.setInputFormatClass(TextInputFormat.class);
		jobSorting.setOutputFormatClass(TextOutputFormat.class);
			
		FileInputFormat.setInputPaths(jobSorting, inputPath);
		FileOutputFormat.setOutputPath(jobSorting, outputPath);
			
		jobSorting.setOutputKeyClass(Text.class);
		jobSorting.setOutputValueClass(Text.class);

		jobSorting.waitForCompletion(true);
	}
}
