package p1;

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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

public class stateFriend {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<Integer, String> stateMap = new HashMap<>();

        protected void setup(Mapper.Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            Path part = new Path(context.getConfiguration().get("textPath"));// Location of file in HDFS (userdata.txt)
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(part);
            for (FileStatus status : fss) {
                Path pt = status.getPath();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String userinfo;
                userinfo = br.readLine();

                while (userinfo != null) {
                    String[] arr = userinfo.split(",");

                    // Put (user#, (name:state)) in the HashMap variable
                    stateMap.put(Integer.parseInt(arr[0]), arr[1]+":"+arr[5]);//key: number, value: name:state
                    userinfo = br.readLine();
                }
            }
        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            String[] friends = data[1].split(",");
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for( String friend : friends){
                sb.append(stateMap.get(Integer.parseInt(friend)));
                sb.append(", ");
            }
            sb.delete(sb.length()-2, sb.length());
            sb.append("]");
            context.write(new Text(data[0]), new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        commonFriend cf = new commonFriend();
        cf.clearFolder("cf");
        cf.clearFolder("sf");
        System.out.print(cf.main(args));
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.err.println("Usage: stateFriend <in> <tmp> <text.file> <out>");
            otherArgs = new String[3];
            otherArgs[0] = "./cf/part-r-00000";
            otherArgs[1] = "./sf/";
            otherArgs[2] = "./input/userdata.txt";
            //System.exit(2);
        }
        conf.set("textPath", otherArgs[2]);
        Job job = new Job(conf, "topTen");
        job.setJarByClass(stateFriend.class);
        job.setMapperClass(Map.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);


        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
