package p1;

import example.avgAge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;

public class maxUser {
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
    public static class Reduce extends Reducer<IntWritable,Text,Text,Text> {

        HashMap<String, String> addressMap = new HashMap<>();
        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path part = new Path(context.getConfiguration().get("textPath"));// Location of file in HDFS
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(part);
            for (FileStatus status : fss) {
                Path pt = status.getPath();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String userinfo;
                userinfo = br.readLine();
                //int year = Calendar.getInstance().get(Calendar.YEAR);
                while (userinfo != null) {//345
                    String[] arr = userinfo.split(",");
                    // Put (user#, age) in the ageMap
                    //String[] data = arr[9].split("/");
                    //int age = year-Integer.parseInt(data[2]);

                    //ageMap.put(arr[0], age);//number birth year
                    addressMap.put(arr[0],arr[3]+","+arr[4]+","+arr[5]);
                    userinfo = br.readLine();
                    //System.out.println(arr[0]+":"+ data[2]);
                }
            }
        }
        int counter = 0;
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Iterator<Text> it = values.iterator();
            while (counter<10 && it.hasNext()){
                String id = it.next().toString();

                context.write(new Text(id+","),new Text(addressMap.get(id)+","+key));
                counter++;
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        clearFolder("maxAge");
        //clearFolder("maxAge_top10");
        //clearFolder("maxAge_top10_user");
        // get all args
        if (otherArgs.length != 3) {
            System.err.println("Usage: avgAge <in> <tmp> <tmp2> <out> <text.file>");
            otherArgs = new String[5];
            otherArgs[0] = "./input/soc-LiveJournal1Adj.txt";
            otherArgs[1] = "./maxAge/";
//            otherArgs[2] = "./maxAge_top10/";
//            otherArgs[3] = "./maxAge_top10_user/";
            otherArgs[2] = "./input/userdata.txt";
            //System.exit(2);
        }
        conf.set("textPath", otherArgs[2]);
        // create a job with name "commonFriend"
        Job job_maxAge = Job.getInstance(conf, "job_maxAge");
        job_maxAge.setJarByClass(maxUser.class);
        job_maxAge.setMapperClass(Map.class);
        job_maxAge.setReducerClass(Reduce.class);
        job_maxAge.setMapOutputKeyClass(IntWritable.class);
        job_maxAge.setMapOutputValueClass(Text.class);
        job_maxAge.setSortComparatorClass(DescendingDoubleComparator.class);
        job_maxAge.setOutputKeyClass(Text.class);
        job_maxAge.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job_maxAge, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job_maxAge, new Path(otherArgs[1]));
        job_maxAge.waitForCompletion(true);
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
