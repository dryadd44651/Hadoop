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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class commonFriend {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        //input type: LongWritable, Text
        //output type: Text, Text

        private Text keyPair = new Text(); // type of output key
        private Text friend = new Text(); // type of output key

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] token = value.toString().split("\t");
            String[] pair = new String[2];
            if(token.length < 2) return;
            String[] data = token[1].split(",");

            for (String d : data) {
                pair[0] = token[0];
                pair[1] = d;
                if(Integer.parseInt(pair[0])<Integer.parseInt(pair[1]))
                    keyPair.set("<"+pair[0]+","+pair[1]+">");
                else
                    keyPair.set("<"+pair[1]+","+pair[0]+">");

                friend.set(token[1]); // set word as each input keyword
                context.write(keyPair, friend); // create a pair <keyword, 1>
            }
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> set = new HashSet<>();
            StringBuilder sb = new StringBuilder();
            Iterator<Text> it = values.iterator();
            //System.out.println(key);
            for (Text val : values) {
                //System.out.println(val);
                String[] token = val.toString().split(",");
                for (String t:token) {
                    if(!set.add(t)){
                        sb.append(t+",");
                    }
                }
            }
            if(sb.length() > 0) {
                sb.deleteCharAt(sb.length()-1);//delete last ","
                context.write(key, new Text(sb.toString()));
            }

        }
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
    // Driver program
    public static void main(String[] args) throws Exception {
        clearFolder("cf");
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: commonFriend <in> <out>");
            otherArgs = new String[2];
            otherArgs[0] = "./input/soc-LiveJournal1Adj.txt";
            otherArgs[1] = "./cf";
            //System.exit(2);

        }

        // create a job with name "commonFriend"
        Job job = new Job(conf, "commonFriend");
        job.setJarByClass(commonFriend.class);
        job.setMapperClass(commonFriend.Map.class);
        job.setReducerClass(commonFriend.Reduce.class);


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
        int result = (job.waitForCompletion(true) ? 0 : 1);
    }
    public int run(String[] args) {
        try {
            main(args);
            return 1;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }

    }

}
