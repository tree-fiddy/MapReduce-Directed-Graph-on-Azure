package edu.gatech.cse6242;
//http://unmeshasreeveni.blogspot.com/2014/04/chaining-jobs-in-hadoop-mapreduce.html;
//https://stackoverflow.com/questions/38111700/chaining-of-mapreduce-jobs
// MapReduce Difference in Count:
// https://stackoverflow.com/questions/47083481/mapreduce-difference-in-count

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/* For Custom Data Type */
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


/* ADDED FOR CHAINING MR JOBS */
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Q4 extends Configured implements Tool {
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(OUTPUT_PATH)))
        {
            /*If exist delete the output path*/
            fs.delete(new Path(OUTPUT_PATH),true);
        }
        Job job1 = Job.getInstance(conf, "Job1");
        job1.setJarByClass(Q4.class);
        job1.setMapperClass(NodeDegreeMapper1.class);
        job1.setCombinerClass(NodeDegreeReducer1.class);
        job1.setReducerClass(NodeDegreeReducer1.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);

        Job job2 = Job.getInstance(conf, "Job2");
        job2.setJarByClass(Q4.class);
        job2.setMapperClass(NodeDegreeMapper2.class);
        job2.setCombinerClass(NodeDegreeReducer2.class);
        job2.setReducerClass(NodeDegreeReducer2.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    } //end main

//	@Override
	public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Job job1 = Job.getInstance(conf, "Job1");
        job1.setJarByClass(Q4.class);
        job1.setMapperClass(NodeDegreeMapper1.class);
        job1.setCombinerClass(NodeDegreeReducer1.class);
        job1.setReducerClass(NodeDegreeReducer1.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Job2");
        job2.setJarByClass(Q4.class);
        job2.setMapperClass(NodeDegreeMapper2.class);
        job2.setCombinerClass(NodeDegreeReducer2.class);
        job2.setReducerClass(NodeDegreeReducer2.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        return job2.waitForCompletion(true) ? 0 : 1;
    }
        //end main

    private static final String OUTPUT_PATH = "./intermediate_output";

    // Custom Data Type
    public static class OutInDegree implements Writable {
        // declare variables
        Integer outDegree;
        Integer inDegree;

        // constructor
        public OutInDegree() {
            outDegree = 0;
            inDegree = 0;
        }

        //set method
        void setOutDegree(Integer id) {
            this.outDegree = id;
        }

        void setInDegree(Integer id) {
            this.inDegree = id;
        }

        //get method
        Integer getOutDegree() {
            return outDegree;
        }

        Integer getInDegree() {
            return inDegree;
        }

        // write method
        public void write(DataOutput out) throws IOException {
            // what order we want to write !
            out.writeInt(outDegree);
            out.writeInt(inDegree);
        }

        // readFields Method
        public void readFields(DataInput in) throws IOException {
            outDegree = new Integer(in.readInt());
            inDegree = new Integer(in.readInt());
        }

        public String toString() {
            return outDegree + "\t" + inDegree;
        }

    }

    // Begin First Mapper
    public static class NodeDegreeMapper1
            extends Mapper<Object, Text, IntWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                IntWritable node = new IntWritable(Integer.parseInt(itr.nextToken()));
                context.write(node, one);
            }
        }
    }

    public static class NodeDegreeReducer1
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    } // end Reducer 1


    public static class NodeDegreeMapper2
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> { 
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            // split each line on tab and save to array
            String line = lineText.toString();
            String[] line_array = line.split("\t");
            int occurrence = Integer.parseInt(line_array[1]);
            context.write(new IntWritable(occurrence), one);
        } // end map
    } // end SecondMapper


    /* For the 2nd Reducer Job */
    // sample input: {3: 1}, {2: 1,1,1}, {1: 1}
    // sample output: {3: 1}, {2: 3}, {1: 1}
    public static class NodeDegreeReducer2
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        } //end reduce
    }

}