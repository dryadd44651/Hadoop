package example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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



/*
 * Job1:
 * 		map:
 * 			fetch directed friend's ages (in-memory join)
 * 		reduce:
 * 			calculate average of directed friend's age. (reducer-side join)
 * Job2:
 * 		map:
 * 			reverse the key and value from the output of 'Job1'
 * 		reduce:
 * 			output top fifteen highest average ages
 * Job3:
 * 		map:
 * 			fetch the address of those top 15 id
 * 
 */


public class avgAge {
	// Job1
    public static class Map extends Mapper<LongWritable, Text, Text, Text>{
    		
    		HashMap<Integer, String> ageMap = new HashMap<>();
		
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
					// Put (user#, age) in the ageMap
					String[] data = arr[9].split("/");
					ageMap.put(Integer.parseInt(arr[0]), data[2]);
					userinfo = br.readLine();
				}
			}    			
		}
    		
    	
    		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                       
            String[] data = value.toString().split("\t");               
            if(data.length<2) return;
            String[] friends = data[1].split(",");
            StringBuilder sb = new StringBuilder();
            for(String friend : friends) {
            		sb.append(2019-Integer.parseInt(ageMap.get(Integer.parseInt(friend)))); 
            		sb.append(",");
            }
            sb.deleteCharAt(sb.length()-1);
            context.write(new Text(data[0]), new Text(sb.toString()));                                
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,DoubleWritable> {       
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
        		double sum = 0.0; // initialize the sum for each keyword
            Iterator<Text> it = values.iterator();
            String[] ages = it.next().toString().split(",");
            for(String age: ages) {
            		sum+=Double.parseDouble(age);
            }
            double avg = sum/ages.length;            
            	context.write(key,new DoubleWritable(avg));
            
        }
    }
    // Job2
    public static class Reverse extends Mapper<LongWritable, Text, DoubleWritable, Text>{        		
    		
    		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {    
            String[] data = value.toString().split("\t");                          
            context.write(new DoubleWritable(Double.parseDouble(data[1])), new Text(data[0])); // create a pair <user friend, friends>                
        }
    }
	
    public static class DescendingDoubleComparator extends WritableComparator {

		public DescendingDoubleComparator() {
			super(DoubleWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleWritable key1 = (DoubleWritable) w1;
			DoubleWritable key2 = (DoubleWritable) w2;          
			return -1 * key1.compareTo(key2);
		}
	}
    
    public static class getFifteen extends Reducer<DoubleWritable,Text,Text,Text> {
		private static int count =15;
		
	    	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	            Iterator<Text> it = values.iterator();
	            while(count > 0) {
	            		StringBuilder sb = new StringBuilder();
	            		sb.append(key.get());
	            		context.write(new Text(it.next().toString()), new Text(sb.toString()));
	            		count--;
	            }
	    	}
    }
    // Job3
    public static class Address extends Mapper<LongWritable, Text, Text, Text>{
    	
		HashMap<Integer, String> addressMap = new HashMap<>();
		
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
					// Put (user#, address) in the HashMap variable
					StringBuilder sb = new StringBuilder();
					for(int i=3; i<8; i++) {
						sb.append(arr[i]);
						sb.append(", ");
					}
					sb.delete(sb.length()-2, sb.length());
					addressMap.put(Integer.parseInt(arr[0]), sb.toString());
					userinfo = br.readLine();
				}
			}    			
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                       
            String[] data = value.toString().split("\t");
            StringBuilder sb = new StringBuilder();
            sb.append(addressMap.get(Integer.parseInt(data[0])));
            sb.append("\t");
            sb.append("avg: ");
            sb.append(data[1]);           
            context.write(new Text(data[0]), new Text(sb.toString()));
    }
}
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 5) {
            System.err.println("Usage: avgAge <in> <tmp> <tmp2> <out> <text.file>");
            System.exit(2);
        }
        conf.set("textPath", otherArgs[4]);
        // create a job with name "commonFriend"
        Job job1 = Job.getInstance(conf, "avgAge");
        job1.setJarByClass(avgAge.class);
        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        
        // set output key type
        job1.setOutputKeyClass(Text.class);
        // set output value type
        job1.setOutputValueClass(DoubleWritable.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
        //Wait till job completion
        job1.waitForCompletion(true);
        
        
        
        Job job2 = Job.getInstance(conf, "avgAge");
        job2.setJarByClass(avgAge.class);
        job2.setMapperClass(Reverse.class);
        job2.setSortComparatorClass(DescendingDoubleComparator.class);
        job2.setReducerClass(getFifteen.class);        
        
        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
        
        job2.waitForCompletion(true);
        
        Job job3 = Job.getInstance(conf, "avgAge");
        job3.setJarByClass(avgAge.class);
        job3.setMapperClass(Address.class);
        job3.setMaxReduceAttempts(0);
        
        
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job3, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]));
        
        
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
