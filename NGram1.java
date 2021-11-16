//cluster = cwtest1
//bucket = rccoursework
//region = us-central1
//jar file = ng.jar
//input = gs://rccoursework/input
//output = gs://rccoursework/output

package mapreduce.donation.totalorderpartitioner;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class NGram {

  public static class NGMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      	String []itr=value.toString().split("\\W+"); 
     	//remove all punctuaion, only use words as key
	//Turn string from book to array
	    
	//loop through array and to add itr according to n
	//n = 1
        for(int i=0;i<(itr.length) - 1; i++){
		word.set(itr[i]+" "+itr[i+1]); 
        	context.write(word, one);  
      }
    }
  }

  public static class NGReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "NGram");
    job.setJarByClass(NGram.class);
    job.setMapperClass(NGMapper.class);
    job.setReducerClass(NGReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  
	InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);
	InputSampler.writePartitionFile(job, sampler);
	  
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
