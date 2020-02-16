package example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import p1.commonFriend;

public class stateFriend {
	
    public static class Map extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                       
            String[] data = value.toString().split("\t");
            if(data.length < 2) return;         
            String[] pair = new String[2];
            String[] friends = data[1].split(",");
            for(String friend : friends) {
            		pair[0] = data[0];
            		pair[1] = friend;
            		Arrays.sort(pair);
                context.write(new Text("("+pair[0]+", "+pair[1]+")"), new Text(data[1])); // create a pair <user friend, friends>                
            }	            
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text> {       
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> set = new HashSet<>();
            StringBuilder sb = new StringBuilder();
            Iterator<Text> it = values.iterator();
            while(it.hasNext()) {
            		String[] friends = it.next().toString().split(",");
            		for(String friend: friends) {
            			if(!set.add(friend)) {
            				sb.append(friend);
                            sb.append(",");
                        }
                    }
            }
            if(sb.length() > 0) {
                    sb.deleteCharAt(sb.length()-1);
                    context.write(key, new Text(sb.toString()));
            }
        }
    }

    public static class State extends Mapper<LongWritable, Text, Text, Text>{

        HashMap<Integer, String> stateMap = new HashMap<>();

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

            while (userinfo != null) {
                    String[] arr = userinfo.split(",");

                    // Put (user#, (name:state)) in the HashMap variable
                    stateMap.put(Integer.parseInt(arr[0]), arr[1]+":"+arr[5]);
                    userinfo = br.readLine();
                    }
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            String[] friends = data[1].split(",");
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for(String friend : friends) {
                sb.append(stateMap.get(Integer.parseInt(friend)));
                sb.append(", ");
            }
            sb.delete(sb.length()-2, sb.length());
            sb.append("]");
            context.write(new Text(data[0]), new Text(sb.toString()));
        }
    }
	
	
	public static void main(String[] args) throws Exception {
        p1.commonFriend cf = new commonFriend();
        cf.clearFolder("cf");
        cf.clearFolder("sf");
        System.out.print(cf.run(args));
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 4) {
            System.err.println("Usage: stateFriend <in> <tmp> <text.file> <out>");
            otherArgs = new String[4];
            otherArgs[0] = "./input/soc-LiveJournal1Adj.txt";
            otherArgs[1] = "./cf/";
            otherArgs[2] = "./sf/";
            otherArgs[3] = "./input/userdata.txt";
            //System.exit(2);
        }
        conf.set("textPath", otherArgs[3]);
        // create a job with name "commonFriend"
//        Job job1 = Job.getInstance(conf, "stateFriend");
//        job1.setJarByClass(stateFriend.class);
//        job1.setMapperClass(Map.class);
//        job1.setReducerClass(Reduce.class);
//
//        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
//
//        // set output key type
//        job1.setOutputKeyClass(Text.class);
//        // set output value type
//        job1.setOutputValueClass(Text.class);
//        //set the HDFS path of the input data
//        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
//        // set the HDFS path for the output
//        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
//        //Wait till job completion
//        job1.waitForCompletion(true);
        
        Job job2 = Job.getInstance(conf, "stateFriend");
        job2.setJarByClass(stateFriend.class);
        job2.setMapperClass(State.class);
        
        job2.setMaxReduceAttempts(0);
        
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+"part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
        
        
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
