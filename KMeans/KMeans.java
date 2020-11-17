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

public class KMeans {

    public static class EuclideanMap extends Mapper<Object, Text, Text, Text> {
        public static double cost = 0.0;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            double[][] centroids = new double[10][58];
            double[] points = new double[58];
            Configuration conf = context.getConfiguration();
            Path p = new Path(conf.get("centroidsInput"));
            int iterationNum = Integer.valueOf(conf.get("iterationNum"));
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
            String line = null;
            int rowNum = 0;

            if(iterationNum == 1) {
                while((line = br.readLine()) != null) {
                    String[] parts = line.split(" ");
                    for(int i = 0; i < 58; i++) centroids[rowNum][i] = Double.valueOf(parts[i]);
                    rowNum ++;
               	}
            } else {
                while((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if(parts.length > 10) {
                        for(int i = 0; i < 58; i++) centroids[rowNum][i] = Double.valueOf(parts[i + 1]);
                        rowNum ++;
                    }
                }
            }

            if(fs != null) fs.close();

            String input_value = value.toString();
            String[] parts = input_value.split(" ");
            String outValue = "";

            for(int i = 0; i < 58; i++) points[i] = Double.valueOf(parts[i]);

            double euclideanDistance = 0.0;
            int groupId = 1;
            for(int i = 0; i < 58; i++) {
                outValue += (" " + Double.toString(points[i])); //data point
                double dist = points[i] - centroids[0][i];
                euclideanDistance += dist * dist;
            }

            double temp = 0.0;
            for(int i = 1; i < rowNum; i++) {
                for(int j = 0; j < 58; j++) {
                    double dist = points[j] - centroids[i][j];
                    temp += dist * dist;
                }

                if(temp < euclideanDistance) {
                    euclideanDistance = temp;
                    groupId = i + 1;
                }
                temp = 0.0;
            }

            cost += euclideanDistance;

            context.write(new Text("COST:"), new Text(Double.toString(cost)));
            context.write(new Text(Integer.toString(groupId)), new Text(outValue.substring(1)));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(key.toString().equals("COST:")) {
                double cost = 0.0;
                for(Text value : values) if(Double.valueOf(value.toString()) > cost) cost = Double.valueOf(value.toString());
                context.write(new Text("COST:"), new Text(Double.toString(cost)));
            } else {
                double[] points = new double[58];
                int pointCounter = 0;
                String outValue = "";

                for(int i = 0; i < 58; i++) points[i] = 0.0;
                for(Text value : values) {
                    String[] parts = value.toString().split(" ");
                    for(int i = 0; i < 58; i++) {
                        points[i] += Double.valueOf(parts[i]);
                    }
                    pointCounter ++;
                }

                for(int i = 0; i < 58; i++) {
                    points[i] /= pointCounter;
                    outValue += ("," + Double.toString(points[i]));
                }

                context.write(key, new Text(outValue));
            }
        }
    }

    public static class ManhattanMap extends Mapper<Object, Text, Text, Text> {
        public static double cost = 0.0;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            double[][] centroids = new double[10][58];
            double[] points = new double[58];
            Configuration conf = context.getConfiguration();
            Path p = new Path(conf.get("centroidsInput"));
            int iterationNum = Integer.valueOf(conf.get("iterationNum"));
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
            String line = null;
            int rowNum = 0;

            if(iterationNum == 1) {
                while((line = br.readLine()) != null) {
                    String[] parts = line.split(" ");
                    for(int i = 0; i < 58; i++) centroids[rowNum][i] = Double.valueOf(parts[i]);
                    rowNum ++;
               	}
            } else {
                while((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if(parts.length > 10) {
                        for(int i = 0; i < 58; i++) centroids[rowNum][i] = Double.valueOf(parts[i + 1]);
                        rowNum ++;
                    }
                }
            }

            if(fs != null) fs.close();

            String input_value = value.toString();
            String[] parts = input_value.split(" ");
            String outValue = "";

            for(int i = 0; i < 58; i++) points[i] = Double.valueOf(parts[i]);

            double manhattanDistance = 0.0;
            int groupId = 1;
            for(int i = 0; i < 58; i++) {
                outValue += (" " + Double.toString(points[i])); //data point
                manhattanDistance += Math.abs(points[i] - centroids[0][i]);
            }

            double temp = 0.0;
            for(int i = 1; i < rowNum; i++) {
                for(int j = 0; j < 58; j++) {
                    temp += Math.abs(points[j] - centroids[i][j]);
                }

                if(temp < manhattanDistance) {
                    manhattanDistance = temp;
                    groupId = i + 1;
                }
                temp = 0.0;
            }

            cost += manhattanDistance;

            context.write(new Text("COST:"), new Text(Double.toString(cost)));
            context.write(new Text(Integer.toString(groupId)), new Text(outValue.substring(1)));
        }
    }

    public static class DistMap extends Mapper<Object, Text, Text, Text> {
        public static double cost = 0.0;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			if(value.toString().split(",").length > 10) context.write(new Text("ClacDist:"), value);
        }
    }

    public static class DistReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double[][] centroids = new double[10][58];
            String[] group = new String[10];
            int rowNum = 0;
            double euclideanDistance = 0.0;
            double manhattanDistance = 0.0;

            for(Text value : values) {
            	String[] parts = value.toString().split(",");
            	if (parts[0].charAt(1) == '0') group[rowNum] = "10";
            	else group[rowNum] = Character.toString(parts[0].charAt(0));

            	for(int i = 0; i < 58; i++) centroids[rowNum][i] = Double.valueOf(parts[i + 1]);
            	rowNum ++;
            }

            for(int i = 0; i < rowNum; i++) {
            	for(int j = i + 1; j < rowNum; j++) {
            		for(int k = 0; k < 58; k++) {
            			double temp = Math.abs(centroids[i][k] - centroids[j][k]);
            			euclideanDistance += temp * temp;
            			manhattanDistance += temp;
            		}
            		context.write(new Text("Euclidean (Group " + group[i] + ", Group " + group[j] + "):"), new Text(Double.toString(euclideanDistance)));
            		context.write(new Text("Manhattan (Group " + group[i] + ", Group " + group[j] + "):"), new Text(Double.toString(manhattanDistance)));
            		euclideanDistance = 0.0;
            		manhattanDistance = 0.0;
            	}
            }
        }
    }

	public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: [input] [output]");
            System.exit(-1);
        }

        for(int i = 1; i <= 20; i++) {
            Path inputPath = new Path(args[0]); //args[0] = "/user/root/data/data.txt"
            Path outputPath = new Path(args[1] + "_c1_" + Integer.toString(i));
            Configuration euclideanConf = new Configuration();

            if(i == 1) euclideanConf.set("centroidsInput", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/data/c1.txt");
            else euclideanConf.set("centroidsInput", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/" + args[1] + "_c1_" + Integer.toString(i - 1) + "/part-r-00000");

            euclideanConf.set("iterationNum", Integer.toString(i));
            euclideanConf.setBoolean("fs.hdfs.impl.disable.cache", true);
            Job jobEuclidean = new Job(euclideanConf, "Euclidean");
            jobEuclidean.setJarByClass(KMeans.class);

            jobEuclidean.setMapperClass(EuclideanMap.class);
            jobEuclidean.setReducerClass(Reduce.class);

            jobEuclidean.setInputFormatClass(TextInputFormat.class);
            jobEuclidean.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(jobEuclidean, inputPath);
            FileOutputFormat.setOutputPath(jobEuclidean, outputPath);

            jobEuclidean.setOutputKeyClass(Text.class);
            jobEuclidean.setOutputValueClass(Text.class);

            jobEuclidean.waitForCompletion(true);
        }

		for(int i = 1; i <= 20; i++) {
             Path inputPath = new Path(args[0]); //args[0] = "/user/root/data/data.txt"
             Path outputPath = new Path(args[1] + "_c2_" + Integer.toString(i));
             Configuration euclideanConf = new Configuration();

             if(i == 1) euclideanConf.set("centroidsInput", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/data/c2.txt");
             else euclideanConf.set("centroidsInput", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/" + args[1] + "_c2_" + Integer.toString(i - 1) + "/part-r-00000");

             euclideanConf.set("iterationNum", Integer.toString(i));
             euclideanConf.setBoolean("fs.hdfs.impl.disable.cache", true);
             Job jobEuclidean = new Job(euclideanConf, "Euclidean");
             jobEuclidean.setJarByClass(KMeans.class);

             jobEuclidean.setMapperClass(EuclideanMap.class);
             jobEuclidean.setReducerClass(Reduce.class);

             jobEuclidean.setInputFormatClass(TextInputFormat.class);
             jobEuclidean.setOutputFormatClass(TextOutputFormat.class);

             FileInputFormat.setInputPaths(jobEuclidean, inputPath);
             FileOutputFormat.setOutputPath(jobEuclidean, outputPath);

             jobEuclidean.setOutputKeyClass(Text.class);
             jobEuclidean.setOutputValueClass(Text.class);

             jobEuclidean.waitForCompletion(true);
        }

        for(int i = 1; i <= 20; i++) { //Manhattan
             Path inputPath = new Path(args[0]); //args[0] = "/user/root/data/data.txt"
             Path outputPath = new Path(args[1] + "_c1_manhattan_" + Integer.toString(i));
             Configuration manhattanConf = new Configuration();

             if(i == 1) manhattanConf.set("centroidsInput", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/data/c1.txt");
             else manhattanConf.set("centroidsInput", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/" + args[1] + "_c1_manhattan_" + Integer.toString(i - 1) + "/part-r-00000");

             manhattanConf.set("iterationNum", Integer.toString(i));
             manhattanConf.setBoolean("fs.hdfs.impl.disable.cache", true);
             Job jobManhattan = new Job(manhattanConf, "Manhattan");
             jobManhattan.setJarByClass(KMeans.class);

             jobManhattan.setMapperClass(ManhattanMap.class);
             jobManhattan.setReducerClass(Reduce.class);

             jobManhattan.setInputFormatClass(TextInputFormat.class);
             jobManhattan.setOutputFormatClass(TextOutputFormat.class);

             FileInputFormat.setInputPaths(jobManhattan, inputPath);
             FileOutputFormat.setOutputPath(jobManhattan, outputPath);

             jobManhattan.setOutputKeyClass(Text.class);
             jobManhattan.setOutputValueClass(Text.class);

             jobManhattan.waitForCompletion(true);
        }

        for(int i = 1; i <= 20; i++) { //Manhattan
             Path inputPath = new Path(args[0]); //args[0] = "/user/root/data/data.txt"
             Path outputPath = new Path(args[1] + "_c2_manhattan_" + Integer.toString(i));
             Configuration manhattanConf = new Configuration();

             if(i == 1) manhattanConf.set("centroidsInput", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/data/c2.txt");
             else manhattanConf.set("centroidsInput", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/" + args[1] + "_c2_manhattan_" + Integer.toString(i - 1) + "/part-r-00000");

             manhattanConf.set("iterationNum", Integer.toString(i));
             manhattanConf.setBoolean("fs.hdfs.impl.disable.cache", true);
             Job jobManhattan = new Job(manhattanConf, "Manhattan");
             jobManhattan.setJarByClass(KMeans.class);

             jobManhattan.setMapperClass(ManhattanMap.class);
             jobManhattan.setReducerClass(Reduce.class);

             jobManhattan.setInputFormatClass(TextInputFormat.class);
             jobManhattan.setOutputFormatClass(TextOutputFormat.class);

             FileInputFormat.setInputPaths(jobManhattan, inputPath);
             FileOutputFormat.setOutputPath(jobManhattan, outputPath);

             jobManhattan.setOutputKeyClass(Text.class);
             jobManhattan.setOutputValueClass(Text.class);

             jobManhattan.waitForCompletion(true);
        }

        {
         Path inputPath = new Path(args[1] + "_c1_20/part-r-00000");
	     Path outputPath = new Path(args[1] + "_euclidean_centroids_c1");
	     Configuration euclideanCentroidsConf = new Configuration();

	     euclideanCentroidsConf.setBoolean("fs.hdfs.impl.disable.cache", true);
	     Job jobEuclideanCentroids = new Job(euclideanCentroidsConf, "Euclidean Centroids c1");
	     jobEuclideanCentroids.setJarByClass(KMeans.class);

	     jobEuclideanCentroids.setMapperClass(DistMap.class);
	     jobEuclideanCentroids.setReducerClass(DistReduce.class);

	     jobEuclideanCentroids.setInputFormatClass(TextInputFormat.class);
	     jobEuclideanCentroids.setOutputFormatClass(TextOutputFormat.class);

	     FileInputFormat.setInputPaths(jobEuclideanCentroids, inputPath);
	     FileOutputFormat.setOutputPath(jobEuclideanCentroids, outputPath);

	     jobEuclideanCentroids.setOutputKeyClass(Text.class);
	     jobEuclideanCentroids.setOutputValueClass(Text.class);

	     jobEuclideanCentroids.waitForCompletion(true);
	 	}

	 	{
		 Path inputPath = new Path(args[1] + "_c1_manhattan_20/part-r-00000");
	     Path outputPath = new Path(args[1] + "_manhattan_centroids_c1");
	     Configuration euclideanCentroidsConf = new Configuration();

	     euclideanCentroidsConf.setBoolean("fs.hdfs.impl.disable.cache", true);
	     Job jobEuclideanCentroids = new Job(euclideanCentroidsConf, "Manhattan Centroids c1");
	     jobEuclideanCentroids.setJarByClass(KMeans.class);

	     jobEuclideanCentroids.setMapperClass(DistMap.class);
	     jobEuclideanCentroids.setReducerClass(DistReduce.class);

	     jobEuclideanCentroids.setInputFormatClass(TextInputFormat.class);
	     jobEuclideanCentroids.setOutputFormatClass(TextOutputFormat.class);

	     FileInputFormat.setInputPaths(jobEuclideanCentroids, inputPath);
	     FileOutputFormat.setOutputPath(jobEuclideanCentroids, outputPath);

	     jobEuclideanCentroids.setOutputKeyClass(Text.class);
	     jobEuclideanCentroids.setOutputValueClass(Text.class);

	     jobEuclideanCentroids.waitForCompletion(true);	     
    	}

    	{
	     Path inputPath = new Path(args[1] + "_c2_20/part-r-00000");
	     Path outputPath = new Path(args[1] + "_euclidean_centroids_c2");
	     Configuration euclideanCentroidsConf = new Configuration();

	     euclideanCentroidsConf.setBoolean("fs.hdfs.impl.disable.cache", true);
	     Job jobEuclideanCentroids = new Job(euclideanCentroidsConf, "Euclidean Centroids c2");
	     jobEuclideanCentroids.setJarByClass(KMeans.class);

	     jobEuclideanCentroids.setMapperClass(DistMap.class);
	     jobEuclideanCentroids.setReducerClass(DistReduce.class);

	     jobEuclideanCentroids.setInputFormatClass(TextInputFormat.class);
	     jobEuclideanCentroids.setOutputFormatClass(TextOutputFormat.class);

	     FileInputFormat.setInputPaths(jobEuclideanCentroids, inputPath);
	     FileOutputFormat.setOutputPath(jobEuclideanCentroids, outputPath);

	     jobEuclideanCentroids.setOutputKeyClass(Text.class);
	     jobEuclideanCentroids.setOutputValueClass(Text.class);

	     jobEuclideanCentroids.waitForCompletion(true);	
	 	}

	    {
	     Path inputPath = new Path(args[1] + "_c2_manhattan_20/part-r-00000");
	     Path outputPath = new Path(args[1] + "_manhattan_centroids_c2");
	     Configuration euclideanCentroidsConf = new Configuration();

	     euclideanCentroidsConf.setBoolean("fs.hdfs.impl.disable.cache", true);
	     Job jobEuclideanCentroids = new Job(euclideanCentroidsConf, "Manhattan Centroids c2");
	     jobEuclideanCentroids.setJarByClass(KMeans.class);

	     jobEuclideanCentroids.setMapperClass(DistMap.class);
	     jobEuclideanCentroids.setReducerClass(DistReduce.class);

	     jobEuclideanCentroids.setInputFormatClass(TextInputFormat.class);
	     jobEuclideanCentroids.setOutputFormatClass(TextOutputFormat.class);

	     FileInputFormat.setInputPaths(jobEuclideanCentroids, inputPath);
	     FileOutputFormat.setOutputPath(jobEuclideanCentroids, outputPath);

	     jobEuclideanCentroids.setOutputKeyClass(Text.class);
	     jobEuclideanCentroids.setOutputValueClass(Text.class);

	     jobEuclideanCentroids.waitForCompletion(true);	
	 	}
    }
}



/*mvn archetype:generate -DgroupId=org.apache.hadoop.examples -DartifactId=kmeansjava -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

yarn jar kmeansjava-1.0-SNAPSHOT.jar org.apache.hadoop.examples.KMeans /user/root/data/data.txt output/out_13

hadoop fs -copyFromLocal c1.txt /user/root/data/

hadoop fs -cat /user/root/output/out_13_manhattan_centroid/*

*/
