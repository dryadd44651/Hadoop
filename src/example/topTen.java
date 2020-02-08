package question2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class topTen {
	// job1 - map, reducer
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {    
		    String[] data = value.toString().split("\t");
		    if(data.length < 2) return;         
		    String[] pair = new String[2];
		    String[] friends = data[1].split(",");
		    for(String friend : friends) {
		    		pair[0] = data[0];
		    		pair[1] = friend;
		    		if(Integer.valueOf(pair[0]) > Integer.valueOf(pair[1])) {
		    			String tmp = pair[0];
		    			pair[0] =pair[1];
		    			pair[1] = tmp;
		    		}
		        context.write(new Text(pair[0]+", "+pair[1]), new Text(data[1])); // create a pair <user friend, friends>
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
    // Job2 - map, reducer, sorter
	
    public static class Reverse extends Mapper<LongWritable, Text, IntWritable, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {    
            String[] data = value.toString().split("\t");     
            String[] friends = data[1].split(",");
            IntWritable count = new IntWritable();
            count.set(friends.length);
            context.write(count, new Text(data[0] + "@" + data[1])); // create a pair <user friend, friends>                
        }
    }

    public static class getTen extends Reducer<IntWritable,Text,Text,Text> {
    		private static int count =10;
	    	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	            Iterator<Text> it = values.iterator();
	            while(count > 0) {
	            		String[] pair = it.next().toString().split("@");
	            		String newKey = pair[0].toString();
	            		context.write(new Text(newKey), new Text(key.get() + "\t" + pair[1].toString()));
	            		count--;
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
	
   
    


    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.err.println("Usage: topTen <in> <tmp> <out>");
            System.exit(2);
        }

        // create a job with name "topTen"
        Job job = Job.getInstance(conf, "topTen");
        job.setJarByClass(topTen.class);
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
        
        job.waitForCompletion(true);
        
        // create a job with name "topTen"
        Job job2 = Job.getInstance(conf, "topTen");
        job2.setJarByClass(topTen.class);
        job2.setMapperClass(Reverse.class);
        job2.setSortComparatorClass(DescendingIntComparator.class);
        job2.setReducerClass(getTen.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

        // set output key type
        job2.setOutputKeyClass(IntWritable.class);
        // set output value type
        job2.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
        
        //Wait till job completion
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}