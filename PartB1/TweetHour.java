import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;  
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetHour {

  public class TweetHourMapper extends
          Mapper<Object, Text, IntWritable, IntWritable> {
  
      private final IntWritable one = new IntWritable(1);
      private final IntWritable length = new IntWritable(1);
 
     private final IntWritable erro = new IntWritable(1);
  
      public void map(Object key, Text value, Context context)
              throws IOException, InterruptedException {
  
 
     	if(value.toString().split(";").length == 4) {
 
        try {   
         	
         	String[] line = value.toString().split(";");
         	
         
         	long x = Long.parseLong(line[0]);
         	int hour = LocalDateTime.ofEpochSecond(x/1000, 0, ZoneOffset.of("-02:00:00")).getHour();
         	length.set(hour);

                 context.write(length, one);
 
 
             //}
         }  catch (NumberFormatException e){
         	
         	erro.set(27);
         	context.write(erro, one);
         	
         
         } 
          }
      }
  }



public class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    
	private IntWritable result = new IntWritable();
   
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
 
    	int sum = 0;
        for (IntWritable value : values) {
            sum = sum + value.get();
        }
               result.set(sum);
        
        context.write(key, result);
    }
}


public class TweetHourjav {

  public static void runJob(String[] input, String output) throws Exception {

        Configuration conf = new Configuration();

    Job job = new Job(conf);
    job.setJarByClass(TweetHourjav.class);
    job.setMapperClass(TweetHourMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(3); 
    job.setCombinerClass(IntSumReducer.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    Path outputPath = new Path(output);
    FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
    FileOutputFormat.setOutputPath(job, outputPath);
    outputPath.getFileSystem(conf).delete(outputPath,true);
    job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
       runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
  }

}




}