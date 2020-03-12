package example;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class commonFriend {

    public static class Map extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                       
            String[] data = value.toString().split("\t");
            if(data.length < 2) return;         
            String[] pair = new String[2];
            String[] friends = data[1].split(",");
            for(String friend : friends) {
            		pair[0] = data[0];//key
            		pair[1] = friend;
            		//Arrays.sort(pair);
                    if(Integer.parseInt(pair[0])<Integer.parseInt(pair[1]))
                        context.write(new Text("("+pair[0]+", "+pair[1]+")"), new Text(data[1])); // create a pair <user friend, friends>
                    else
                        context.write(new Text("("+pair[1]+", "+pair[0]+")"), new Text(data[1])); // create a pair <user friend, friends>
            		//System.out.println("("+pair[0]+", "+pair[1]+")"+data[1]);
                    //context.write(new Text("("+pair[0]+", "+pair[1]+")"), new Text(data[1])); // create a pair <user friend, friends>
            }	            
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text> {       
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> set = new HashSet<>();
            StringBuilder sb = new StringBuilder();
            Iterator<Text> it = values.iterator();
            //System.out.println(key);
            while(it.hasNext()) {
            		String[] friends = it.next().toString().split(",");
            		for(String friend: friends) {
            			if(!set.add(friend)) {
            				sb.append(friend);
            				sb.append(",");
            			}
            		}
                System.out.println(set);
            }
            if(sb.length() > 0) {
            		sb.deleteCharAt(sb.length()-1);
            		context.write(key, new Text(sb.toString()));
            }
        }
    }

    public static void clearFolder(String path){
        File file = new File("./"+path);

        File[] listFiles = file.listFiles();
        if(file.isDirectory() == true) {
            for (File f : listFiles) {
                //System.out.println("Deleting " + f.getName());
                f.delete();
            }
            file.delete();
        }

    }
    // Driver program
    public static void main(String[] args) throws Exception {
        clearFolder("cf");
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: WordCount <in> <out>");
            otherArgs = new String[2];
            otherArgs[0] = "./input/soc-LiveJournal1Adj.txt";
            otherArgs[1] = "./cf";
            //System.exit(2);

        }

        // create a job with name "commonFriend"
        Job job = Job.getInstance(conf, "commonFriend");
        job.setJarByClass(commonFriend.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

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