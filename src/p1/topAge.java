package p1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;

public class topAge {
    public static class Map extends Mapper<LongWritable, Text, IntWritable,Text>{
        HashMap<String, Integer> ageMap = new HashMap<>();
        //HashMap<String, String> addressMap = new HashMap<>();
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path part = new Path(context.getConfiguration().get("textPath"));// Location of file in HDFS
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(part);
            for (FileStatus status : fss) {
                Path pt = status.getPath();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String userinfo;
                userinfo = br.readLine();
                int year = Calendar.getInstance().get(Calendar.YEAR);
                while (userinfo != null) {//345
                    String[] arr = userinfo.split(",");
                    // Put (user#, age) in the ageMap
                    String[] data = arr[9].split("/");
                    int age = year-Integer.parseInt(data[2]);

                    ageMap.put(arr[0], age);//number birth year
                    //addressMap.put(arr[0],arr[3]+","+arr[4]+","+arr[5]);
                    userinfo = br.readLine();
                    //System.out.println(arr[0]+":"+ data[2]);
                }
            }
        }


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            if(data.length<2) return;
            String[] friends = data[1].split(",");

            int maxAge = -1;
            for(String friend : friends) {

                if(ageMap.get(friend)>maxAge)
                    maxAge = ageMap.get(friend);
            }
            IntWritable age = new IntWritable();;
            age.set(maxAge);
            context.write(age,new Text(data[0]));
        }
    }
    public static class DescendingDoubleComparator extends WritableComparator {
        public DescendingDoubleComparator() {
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
    public static class Reduce extends Reducer<IntWritable,Text,Text,IntWritable> {
        int counter = 0;
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Iterator<Text> it = values.iterator();
            while (counter<10 && it.hasNext()){
                String id = it.next().toString();

                context.write(new Text(id),key);
                counter++;
            }

        }
    }
    //===
    public static class friendMap extends Mapper<LongWritable, Text, Text,Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            if(data.length<2) return;
            context.write(new Text(data[0]),new Text(data[1]));
        }
    }
    public static class userMap extends Mapper<LongWritable, Text, Text,Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            if(data.length<2) return;
            //int year = Calendar.getInstance().get(Calendar.YEAR);
            //String[] date = data[9].split("/");
            //int age = year-Integer.parseInt(date[2]);
            context.write(new Text(data[0]),new Text(data[3]+","+data[4]+","+data[5]+","));
        }
    }
    public static class joinReduce extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //Iterator<Text> itKey = keys.iterator();
            Iterator<Text> it = values.iterator();
            String[] keyToken = key.toString().split(",");
            String[] data = new String[2];

            int i = 0;
            while (it.hasNext()){
                data[i] = it.next().toString();
                System.out.println(data[i]);
                i++;
            }


            if(i<2) return;
            if(data[0].length()>data[1].length())
                context.write(new Text(keyToken[0]),new Text(data[0]+data[1]));
            else
                context.write(new Text(keyToken[0]),new Text(data[1]+data[0]));



        }
    }
    public static class petitioner extends Partitioner<Text,Text> {
        public int getPartition(Text key, Text value, int numReduceTasks){
            String[] data = key.toString().split(",");
            System.out.println(data[0]);
            return Integer.parseInt(data[0])% numReduceTasks;
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        clearFolder("topAge");
        clearFolder("topAge_user");
        if (otherArgs.length != 3) {
            System.err.println("Usage: avgAge <in> <tmp> <tmp2> <out> <text.file>");
            otherArgs = new String[5];
            otherArgs[0] = "./input/soc-LiveJournal1Adj.txt";
            otherArgs[1] = "./topAge/";
            otherArgs[2] = "./input/userdata.txt";
            otherArgs[3] = "./topAge_user/";
            //System.exit(2);
        }
        conf.set("textPath", otherArgs[2]);
        // create a job with name "commonFriend"
        Job maxAge = Job.getInstance(conf, "job");
        maxAge.setJarByClass(topAge.class);
        maxAge.setMapperClass(Map.class);
        maxAge.setMapOutputKeyClass(IntWritable.class);
        maxAge.setMapOutputValueClass(Text.class);
        maxAge.setSortComparatorClass(DescendingDoubleComparator.class);
        maxAge.setReducerClass(Reduce.class);
        FileInputFormat.addInputPath(maxAge, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(maxAge, new Path(otherArgs[1]));
        maxAge.waitForCompletion(true);


        //job2
        Job job = Job.getInstance(conf, "job");
        job.setJarByClass(topAge.class);
        //job.setMapperClass(Map.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[1]+"part-r-00000"), TextInputFormat.class, friendMap.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[2]),TextInputFormat.class, userMap.class);

        job.setReducerClass(joinReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //job.setSortComparatorClass(maxUser.DescendingDoubleComparator.class);
        job.setPartitionerClass(petitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        job.waitForCompletion(true);
    }

    public static void clearFolder(String path){
        File file = new File("./"+path);

        File[] listFiles = file.listFiles();
        if(file.isDirectory() == true) {
            for (File f : listFiles) {
                System.out.println("Deleting " + f.getName());
                f.delete();
            }
            file.delete();
        }
    }
}
