package p1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class topTen {
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            String[] friends = data[1].split(",");
            IntWritable count = new IntWritable();
            count.set(friends.length);
            //System.out.println(count);
            //System.out.println(data[0]+"@"+data[1]);
            context.write(count, new Text(data[0]+"@"+data[1]));
        }
    }
    public static class Reduce extends Reducer<IntWritable,Text,Text,Text> {
        int counter = 0;
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //System.out.println("reduce");
            Iterator<Text> it = values.iterator();
            String[] result;
            while(it.hasNext()&&counter<10) {

                //System.out.println(text);
                result = it.next().toString().split("@");
                context.write(new Text(result[0]), new Text(result[1]));
                counter++;
            }

        }
    }

    public static class DescendingIntComparator extends WritableComparator {
        public DescendingIntComparator() {
            super(IntWritable.class, true);
        }
        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable key1 = (IntWritable) w1;
            IntWritable key2 = (IntWritable) w2;

            return -1 * key1.compareTo(key2);
        }

    }

    public static void main(String[] args) throws Exception {
        commonFriend cf = new commonFriend();
        cf.clearFolder("cf");
        cf.clearFolder("tt");
        System.out.print(cf.run(args));
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.err.println("Usage: TopTen <in> <cf> <out>");
            otherArgs = new String[3];
            otherArgs[0] = "./input/soc-LiveJournal1Adj.txt";
            otherArgs[1] = "./cf/part-r-00000";
            otherArgs[2] = "./tt";
            //System.exit(2);
        }
        Job job = new Job(conf, "topTen");
        job.setJarByClass(topTen.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setSortComparatorClass(DescendingIntComparator.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);


        // set output key type
        job.setOutputKeyClass(IntWritable.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
