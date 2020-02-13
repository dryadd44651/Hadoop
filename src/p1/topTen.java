package p1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
    public static class Map extends Mapper<Text, Text, IntWritable, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            String[] friends = data[1].split(",");
            IntWritable count = new IntWritable();
            count.set(friends.length);
            System.out.println(count);
            System.out.println(data[0]+"@"+data[1]);
            context.write(count, new Text(data[0]+"@"+data[1]));
        }
    }
    public static class Reduce extends Reducer<IntWritable,Text,Text,Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("reduce");
            Iterator<Text> it = values.iterator();
            Text text = null;
            while(it.hasNext()) {
                text = it.next();
                System.out.println(text);

            }

        }
    }
    public static void main(String[] args) throws Exception {
        commonFriend cf = new commonFriend();
        cf.main(args);
        cf.clearFolder("output");
        cf.clearFolder("tmp");
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.err.println("Usage: WordCount <in> <out>");
            otherArgs = new String[2];
            otherArgs[0] = "./input/soc-LiveJournal1Adj.txt";
            otherArgs[1] = "./tmp";
            otherArgs[1] = "./output";
            //System.exit(2);
        }
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(topTen.class);
        job.setMapperClass(topTen.Map.class);
        job.setReducerClass(topTen.Reduce.class);


        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);


        // set output key type
        job.setOutputKeyClass(Text.class);
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
