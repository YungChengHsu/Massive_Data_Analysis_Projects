package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LSH {

	public static class Map extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String input_value = value.toString();
			if(!input_value.equals("\n")) {
				String[] parts = input_value.replaceAll("[^a-zA-Z0-9\\s]+", "").toLowerCase().split(" ");
				String outKey = "";

				Configuration conf = context.getConfiguration();
				int fileNum = Integer.valueOf(conf.get("fileCounter"));

					if(parts.length > 3) {
						for(int i = 0; i <= parts.length - 3; i++) {
							outKey += parts[i];
							for(int j = i + 1; j < i + 3; j++) {
								outKey += ("/" + parts[j]);
							}
							context.write(new Text(outKey), new Text(Integer.toString(fileNum)));
							outKey = "";
						}
					} else if(parts.length > 0 && parts.length <= 3) {
						outKey += parts[0];
						for(int i = 1 ; i < parts.length; i++) outKey += ("/" + parts[i]);

						context.write(new Text(outKey), new Text(Integer.toString(fileNum)));
					}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text value : values) {
				context.write(key, value);
			}
		}
	}

	public static class ShinglingMap extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String input_value = value.toString();
			String[] parts = input_value.replaceAll("\"", "").split("\t");
			String[] filter = parts[0].split("/");

			if(filter.length >= 3 && !filter[0].equals("")) {
				context.write(new Text(parts[0]), new Text(parts[1]));
			}
		}
	}

	public static class ShinglingReduce extends Reducer<Text, Text, Text, Text> {
		static int shinglesNum = 1;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] set = new String[50];
			String outValue = "";
			for(int i = 0; i < 50; i++) set[i] = "0";
			for(Text value : values) {
				set[Integer.parseInt(value.toString()) - 1] = "1";
			}

			for(int i = 0; i < 50; i++) outValue += ("," + set[i]);

			context.write(key, new Text(outValue));
		}
	}

	public static class MinHashingMap extends Mapper<Object, Text, Text, Text> {
		static int minHashingMapperCounter = 12438; //shingleNum
		static int[] randomA = new int[100];
		static int[] randomB = new int[100];
		static int randomPrime = 21191;
		static Random rand = new Random();
		static {
			for(int i = 0; i < 100; i++) {
				randomA[i] = rand.nextInt(900) + 1;
				randomB[i] = rand.nextInt(2000) + 1;
			}
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String input_value = value.toString();
			String[] parts = input_value.split(",");

			String outKey = "";
			String outValue = "";

			for(int perm = 0; perm < 100; perm++) {
				outKey = "minhashing_" + Integer.toString(perm + 1);
				int hashNum = ((randomA[perm] + minHashingMapperCounter * randomB[perm]) % randomPrime) % 12438 + 1; //here to store the permutated number (% shinglesNum)
				for(int i = 1; i <= 50; i++) {
					if(parts[i].equals("1")) {
						outValue += ("," + Integer.toString(hashNum));
					} else {
						outValue += (",X");
					}
				}
				context.write(new Text(outKey), new Text(outValue.substring(1)));
				outKey = "";
				outValue = "";
			}

			minHashingMapperCounter --;
		}
	}

	public static class MinHashingReduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] result =  new String[50];
			String outValue = "";
			for(int i = 0; i < 50; i++) result[i] = "X";

			for(Text value : values) {
				String[] parts = value.toString().split(",");
					for(int i = 0; i < 50; i++) {
						if(!parts[i].equals("X")) {
							if(result[i].equals("X")) result[i] = parts[i];
							else {
								if(Integer.valueOf(parts[i]) < Integer.valueOf(result[i])) result[i] = parts[i];
							}
						}
					}
			}

			for(int i = 0; i < 50; i++) {
				outValue += ("," + result[i]);
			}

			context.write(key, new Text(outValue));
		}
	}

	public static class LSHMap extends Mapper<Object, Text, Text, Text> {
		static int lshRow = 0;
		static String[][] lshHashing = new String[2][101];

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String input_value = value.toString();
			String[] parts = input_value.split(",");
			String outKey = "";
			Hashtable lshht = new Hashtable();

			for(int i = 0; i < 50; i++) {
				lshHashing[lshRow][i] = parts[i + 1];
			}
			lshRow ++;

			if(lshRow >= 2) {
				lshRow = 0;
				for(int i = 0; i < 50; i++) {
					String lshHashingKey = lshHashing[0][i] + "/" + lshHashing[1][i];
					if(lshht.containsKey(lshHashingKey)) lshht.put(lshHashingKey, lshht.get(lshHashingKey) + "," + Integer.toString(i + 1));
					else lshht.put(lshHashingKey, Integer.toString(i + 1));
				}

				Enumeration buckets = lshht.keys();

				while(buckets.hasMoreElements()) {
					String str = (String) buckets.nextElement();
					String[] inBucket = lshht.get(str).toString().split(",");
					if(inBucket.length > 1) {
						for(int i = 0; i < inBucket.length; i++) {
							for(int j = i + 1; j < inBucket.length; j++) {
								context.write(new Text(inBucket[i] + "/" + inBucket[j]), new Text(Integer.toString(1)));
							}
						}
					}
				}
			}
		}
	}

	public static class LSHReduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.write(key, new Text(Integer.toString(1)));
		}
	}

	public static class NodeForSorting {
		public String pair;
		public double similarity;
	}

    public static class LSHComparator implements Comparator<NodeForSorting> {

        @Override
        public int compare(NodeForSorting v1, NodeForSorting v2) {
            if(v1.similarity < v2.similarity) return 1;

            return -1;
        }
    }

	public static class TopKMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\t");
            context.write(new Text("sorting"), new Text(parts[0]));
        }
    }

    public static class TopKReducer
            extends Reducer<Text, Text, Text, Text> {

        private PriorityQueue<NodeForSorting> TopKRecords;
        String[][] shinglingResult = new String[12438][50];
        static int rollNum = 0;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Comparator<NodeForSorting> comparator = new LSHComparator();
            TopKRecords = new PriorityQueue<NodeForSorting>(comparator);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
			Path p = new Path(conf.get("shinglesInput"));
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
			String line = null;

			while((line = br.readLine()) != null) {
				String[] parts = line.split(",");
				for(int i = 0; i < 50; i++) shinglingResult[rollNum][i] = parts[i + 1];
				rollNum ++;
			}

            for (Text value : values) {
            	String[] parts = value.toString().split("/");
            	Double d = 0.0, n = 0.0;
            	for(int i = 0; i < 12384; i++) {
            		if(shinglingResult[i][Integer.valueOf(parts[0]) - 1].equals("1") || shinglingResult[i][Integer.valueOf(parts[1]) - 1].equals("1")) d++;
            		if(shinglingResult[i][Integer.valueOf(parts[0]) - 1].equals("1") && shinglingResult[i][Integer.valueOf(parts[1]) - 1].equals("1")) n++;
            	}

            	NodeForSorting node = new NodeForSorting();
            	node.pair = "(" + parts[0] + "," + parts[1] + "):";
            	node.similarity = n / d;
            	TopKRecords.add(node);
            }

            for(int i = 1; i <= 10; i++) {
            	NodeForSorting nfs = TopKRecords.poll();
            	if(nfs != null) {
                    double similarityToReducer = nfs.similarity;
                    context.write(new Text(String.valueOf(i)), new Text(nfs.pair + "\t" + Double.toString(similarityToReducer)));
                }
            }
        }
    }

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		for(int i = 1; i <= 50; i++) {
			Path inputPath;
			if(i < 10) inputPath = new Path(args[0] + "/00" + Integer.toString(i) + ".txt");
			else inputPath = new Path(args[0] + "/0" + Integer.toString(i) + ".txt");

			Path outputPath = new Path(args[1] + "_" + Integer.toString(i));
			Configuration preShinglingConf = new Configuration();
			preShinglingConf.set("fileCounter", Integer.toString(i));
			Job jobPreShingling = new Job(preShinglingConf, "Pre-Shingling");
		    jobPreShingling.setJarByClass(LSH.class);
		    jobPreShingling.setMapperClass(Map.class);
		    jobPreShingling.setReducerClass(Reduce.class);
		    jobPreShingling.setInputFormatClass(TextInputFormat.class);
			jobPreShingling.setOutputFormatClass(TextOutputFormat.class);
		    jobPreShingling.setOutputKeyClass(Text.class);
		    jobPreShingling.setOutputValueClass(Text.class);

		    FileInputFormat.setInputPaths(jobPreShingling, inputPath);
		    FileOutputFormat.setOutputPath(jobPreShingling, outputPath);
			jobPreShingling.waitForCompletion(true);
		}


		Path outputPath = new Path(args[1] + "_shingling");
		Configuration shinglingConf = new Configuration();
		Job jobShingling = new Job(shinglingConf, "Shingling");
		jobShingling.setJarByClass(LSH.class);
		jobShingling.setMapperClass(ShinglingMap.class);
		jobShingling.setReducerClass(ShinglingReduce.class);
		jobShingling.setInputFormatClass(TextInputFormat.class);
		jobShingling.setOutputFormatClass(TextOutputFormat.class);
		jobShingling.setOutputKeyClass(Text.class);
		jobShingling.setOutputValueClass(Text.class);

		for(int i = 1; i <= 50; i++) {
		    MultipleInputs.addInputPath(jobShingling, new Path(args[1] + "_" + Integer.toString(i) + "/part-r-00000"), TextInputFormat.class);
		}

		FileOutputFormat.setOutputPath(jobShingling, outputPath);
		jobShingling.waitForCompletion(true);


		Path inputPathSig = new Path(args[1] + "_shingling/part-r-00000");
		Path outputPathSig = new Path(args[1] + "_mhSig");

		Job jobSignature = Job.getInstance(new Configuration());
		jobSignature.setJarByClass(LSH.class);
		jobSignature.setJobName("Signature");

		jobSignature.setMapperClass(MinHashingMap.class);
		jobSignature.setReducerClass(MinHashingReduce.class);

		jobSignature.setInputFormatClass(TextInputFormat.class);
		jobSignature.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobSignature, inputPathSig);
		FileOutputFormat.setOutputPath(jobSignature, outputPathSig);

		jobSignature.setOutputKeyClass(Text.class);
		jobSignature.setOutputValueClass(Text.class);

		jobSignature.waitForCompletion(true);


		//LSH
		Path inputPathLSH = new Path(args[1] + "_mhSig/part-r-00000");
		Path outputPathLSH = new Path(args[1] + "_lsh");

		Configuration lshConf = new Configuration();
		Job jobLSH = new Job(lshConf, "LSH");
		jobLSH.setJarByClass(LSH.class);

		jobLSH.setMapperClass(LSHMap.class);
		jobLSH.setReducerClass(LSHReduce.class);

		jobLSH.setInputFormatClass(TextInputFormat.class);
		jobLSH.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobLSH, inputPathLSH);
		FileOutputFormat.setOutputPath(jobLSH, outputPathLSH);

		jobLSH.setOutputKeyClass(Text.class);
		jobLSH.setOutputValueClass(Text.class);

		jobLSH.waitForCompletion(true);


		Path inputPathSorting = new Path(args[1] + "_lsh/part-r-00000");
		Path outputPathSorting = new Path(args[1] + "_final");

		Configuration sortingConf = new Configuration();
		sortingConf.set("shinglesInput", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/" + args[1] + "_shingling/part-r-00000");
		Job jobSorting = new Job(sortingConf, "Sorting");
		jobSorting.setJarByClass(LSH.class);

		jobSorting.setMapperClass(TopKMapper.class);
		jobSorting.setReducerClass(TopKReducer.class);

		jobSorting.setInputFormatClass(TextInputFormat.class);
		jobSorting.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobSorting, inputPathSorting);
		FileOutputFormat.setOutputPath(jobSorting, outputPathSorting);

		jobSorting.setOutputKeyClass(Text.class);
		jobSorting.setOutputValueClass(Text.class);

		jobSorting.waitForCompletion(true);
	}
}

/*
hadoop fs -copyFromLocal prInput.txt /user/root/data/

hadoop fs -ls -R /user/root/output

mvn archetype:generate -DgroupId=org.apache.hadoop.examples -DartifactId=pagerankjava -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

yarn jar findingsimilaritemsjava-1.0-SNAPSHOT.jar org.apache.hadoop.examples.FindingSimilarItems /user/root/data/athletics output/out_shingling_54

hadoop fs -cat /user/root/output/out_shingling_54_lsh/*

hadoop fs -cat /user/root/output/out_shingling_41/part-r-00000

hadoop fs -cat /user/root/output/out_shingling_51_final/*

hadoop fs -rm -R /user/root/output/out_shingling_51_final

12438
*/
