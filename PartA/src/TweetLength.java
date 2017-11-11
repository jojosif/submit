import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TweetLength {

public class TweetLengthMapper extends
        Mapper<Object, Text, IntWritable, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private final IntWritable length = new IntWritable(1);
    private final IntWritable tir = new IntWritable(1);

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        if(value.toString().split(";").length == 4) {

            String[] line = value.toString().split(";");
            length.set(line[2].length());
            double number = line[2].length() ;
            tir.set((int) Math.ceil(number / 5));
            if (number < 141){
                context.write(tir, one);


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



public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args)
			.getRemainingArgs();
	if (otherArgs.length != 2) {
		System.err.println("<in> <out>");
		System.exit(2);
	}
	Job job = new Job(conf);
	job.setJarByClass(TweetLength.class);
	job.setMapperClass(TweetLengthMapper.class);
	job.setCombinerClass(IntSumReducer.class);
	job.setReducerClass(IntSumReducer.class);
	job.setNumReduceTasks(3);
	job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}