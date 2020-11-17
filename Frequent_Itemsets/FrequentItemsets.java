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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FrequentItemsets {

    public static class FrequentItemsMap extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String input_value = value.toString();
            String[] parts = input_value.split(",");
            String outValue = "";

            for(int i = 0; i < parts.length; i++) context.write(new Text(parts[i]), new Text("1"));
        }
    }

    public static class FrequentItemsReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int threshold = Integer.valueOf(conf.get("threshold"));
            int basketNum = Integer.valueOf(conf.get("basketNum"));
            int counter = 0;

            for(Text value : values) {
                counter += Integer.valueOf(value.toString());
            }

            if(counter >= threshold && counter < (int)Math.ceil(0.7 * basketNum)) context.write(key, new Text(Integer.toString(counter)));
        }
    }

    public static class FrequentPairsMap extends Mapper<Object, Text, Text, Text> {
        LinkedList<LinkedList<String>> tuples = new LinkedList<LinkedList<String>>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path p = new Path(conf.get("frequentItems"));
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
            String line = null;

            LinkedList<String> itemsTemp = new LinkedList<String>();
            
            while((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                itemsTemp.add(parts[0]);
            }

            if(fs != null) fs.close();

            int len = itemsTemp.size();
            for(int i = 0; i < len; i++) {
                for(int j = i + 1; j < len; j++) {
                    LinkedList<String> temp = new LinkedList<String>();
                    temp.add(itemsTemp.get(i));
                    temp.add(itemsTemp.get(j));
                    tuples.add(temp);
                }
            }           
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String input_value = value.toString();
            String[] parts = input_value.split(",");
            Set<String> set = new HashSet<String>();
            for(int i = 0; i < parts.length; i++) set.add(parts[i]);

            int tuplesLen = tuples.size();
            boolean flag;
            for(int i = 0; i < tuplesLen; i++) {
                flag = true;
                int innerTuplesLen = tuples.get(i).size();
                for(int j = 0; j < innerTuplesLen; j++) {
                    if(!set.contains(tuples.get(i).get(j))) {
                        flag = false;
                        break;
                    }
                }
                if(flag) {
                    String outValue = "";
                    for(int j = 0; j < innerTuplesLen; j++) {
                        outValue += ("," + tuples.get(i).get(j));
                    }
                    context.write(new Text(outValue.substring(1)), new Text("1"));
                }
            }
        }
    }

    public static class FrequentPairsReduce extends Reducer<Text, Text, Text, Text> {
        MultipleOutputs mos;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int threshold = Integer.valueOf(conf.get("threshold"));
            int counter = 0;

            for(Text value : values) {
                counter += Integer.valueOf(value.toString());
            }

            context.write(key, new Text(Integer.toString(counter)));
            if(counter >= threshold) mos.write("frequent", key, new Text(Integer.toString(counter)));
            else mos.write("nonFrequent", key, new Text(Integer.toString(counter)));
        }
    }

    public static class FrequentTriplesMap extends Mapper<Object, Text, Text, Text> {
        LinkedList<LinkedList<String>> tuples = new LinkedList<LinkedList<String>>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path p = new Path(conf.get("frequentItems"));
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
            String line = null;

            LinkedList<String> itemsTemp = new LinkedList<String>();
            
            while((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                itemsTemp.add(parts[0]);
            }

            p = new Path(conf.get("non-frequentPairs"));
            fs = FileSystem.get(conf);
            br = new BufferedReader(new InputStreamReader(fs.open(p)));
            line = null;

            LinkedList<LinkedList<String>> nonFrequentPairsTemp = new LinkedList<LinkedList<String>>();
            
            while((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                String[] pairs = parts[0].split(",");
                LinkedList<String> loS = new LinkedList<String>();
                loS.add(pairs[0]);
                loS.add(pairs[1]);
                nonFrequentPairsTemp.add(loS);
            }

            if(fs != null) fs.close();

            int len = itemsTemp.size();
            for(int i = 0; i < len; i++) {
                for(int j = i + 1; j < len; j++) {
                    for(int k = j + 1; k < len; k++) {
                        LinkedList<String> temp = new LinkedList<String>();
                        
                        int nonFreqLen = nonFrequentPairsTemp.size();
                        boolean flag = true;
                        for(int l = 0; l < nonFreqLen; l++) {
                            if( (nonFrequentPairsTemp.get(l).get(0).equals(itemsTemp.get(i)) || nonFrequentPairsTemp.get(l).get(0).equals(itemsTemp.get(j)) || nonFrequentPairsTemp.get(l).get(0).equals(itemsTemp.get(k)))
                                &&
                                (nonFrequentPairsTemp.get(l).get(1).equals(itemsTemp.get(i)) || nonFrequentPairsTemp.get(l).get(1).equals(itemsTemp.get(j)) || nonFrequentPairsTemp.get(l).get(1).equals(itemsTemp.get(k)))
                            ) {
                                flag = false;
                                break;
                            }
                        }

                        if(flag) {
                            temp.add(itemsTemp.get(i));
                            temp.add(itemsTemp.get(j));
                            temp.add(itemsTemp.get(k));
                            tuples.add(temp);
                        } 
                    }
                }
            }         
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String input_value = value.toString();
            String[] parts = input_value.split(",");
            Set<String> set = new HashSet<String>();
            for(int i = 0; i < parts.length; i++) set.add(parts[i]);

            int tuplesLen = tuples.size();
            boolean flag;
            for(int i = 0; i < tuplesLen; i++) {
                flag = true;
                int innerTuplesLen = tuples.get(i).size();
                for(int j = 0; j < innerTuplesLen; j++) {
                    if(!set.contains(tuples.get(i).get(j))) {
                        flag = false;
                        break;
                    }
                }
                if(flag) {
                    String outValue = "";
                    for(int j = 0; j < innerTuplesLen; j++) {
                        outValue += ("," + tuples.get(i).get(j));
                    }
                    context.write(new Text(outValue.substring(1)), new Text("1"));
                }
            }
        }
    }

    public static class FrequentTriplesReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int threshold = Integer.valueOf(conf.get("threshold"));
            int counter = 0;

            for(Text value : values) {
                counter += Integer.valueOf(value.toString());
            }

            if(counter >= threshold) context.write(key, new Text(Integer.toString(counter)));
        }
    }

	public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: [threshold] [execNum] [basketNum]");
            System.exit(-1);
        }

        Path inputPath = new Path("/user/root/data/testInput.txt");
        Path outputPath = new Path("output/frequentItems_" + args[1]);
        Configuration Conf = new Configuration();

        Conf.set("threshold", args[0]);
        Conf.set("basketNum", args[2]);
        Conf.setBoolean("fs.hdfs.impl.disable.cache", true);

        Job job = new Job(Conf, "FrequentItems");
        job.setJarByClass(FrequentItemsets.class);

        job.setMapperClass(FrequentItemsMap.class);
        job.setReducerClass(FrequentItemsReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
        

        inputPath = new Path("/user/root/data/testInput.txt");
        outputPath = new Path("output/out_pairs_" + args[1]);
        Configuration pairsConf = new Configuration();

        pairsConf.set("frequentItems", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/output/frequentItems_" + args[1] + "/part-r-00000");
        pairsConf.set("threshold", args[0]);
        pairsConf.setBoolean("fs.hdfs.impl.disable.cache", true);

        Job pairsJob = new Job(pairsConf, "FrequentItemsets");
        pairsJob.setJarByClass(FrequentItemsets.class);

        pairsJob.setMapperClass(FrequentPairsMap.class);
        pairsJob.setReducerClass(FrequentPairsReduce.class);

        pairsJob.setInputFormatClass(TextInputFormat.class);
        pairsJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(pairsJob, inputPath);
        FileOutputFormat.setOutputPath(pairsJob, outputPath);
        MultipleOutputs.addNamedOutput(pairsJob, "frequent", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(pairsJob, "nonFrequent", TextOutputFormat.class, Text.class, Text.class);

        pairsJob.setOutputKeyClass(Text.class);
        pairsJob.setOutputValueClass(Text.class);

        pairsJob.waitForCompletion(true);


        inputPath = new Path("/user/root/data/testInput.txt");
        outputPath = new Path("output/out_frequent_triples_" + args[1]);
        Configuration triplesConf = new Configuration();

        triplesConf.set("frequentItems", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/output/frequentItems_" + args[1] + "/part-r-00000");
        triplesConf.set("non-frequentPairs", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/output/out_pairs_" + args[1] + "/nonFrequent-r-00000");
        triplesConf.set("threshold", args[0]);
        triplesConf.setBoolean("fs.hdfs.impl.disable.cache", true);

        Job triplesJob = new Job(triplesConf, "FrequentItemsets");
        triplesJob.setJarByClass(FrequentItemsets.class);

        triplesJob.setMapperClass(FrequentTriplesMap.class);
        triplesJob.setReducerClass(FrequentTriplesReduce.class);

        triplesJob.setInputFormatClass(TextInputFormat.class);
        triplesJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(triplesJob, inputPath);
        FileOutputFormat.setOutputPath(triplesJob, outputPath);

        triplesJob.setOutputKeyClass(Text.class);
        triplesJob.setOutputValueClass(Text.class);

        triplesJob.waitForCompletion(true);
    }
}


/*mvn archetype:generate -DgroupId=org.apache.hadoop.examples -DartifactId=frequentitemsetsjava -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

yarn jar frequentitemsetsjava-1.0-SNAPSHOT.jar org.apache.hadoop.examples.FrequentItemsets 3 6 9

hadoop fs -copyFromLocal testInput.txt /user/root/data/

hadoop fs -cat /user/root/output/out_frequent_triples_6/*

*/
