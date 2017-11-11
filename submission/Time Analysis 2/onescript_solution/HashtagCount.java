import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
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

public class HashtagCount {

  public static class TokenizerMapper extends
			Mapper<Object, Text, Text, LongWritable> {

		final static Pattern TAG_PATTERN = Pattern.compile("#[a-zA-Z0-9_]+.");
		private final static LongWritable ONE = new LongWritable(1L);
		private final IntWritable length = new IntWritable(1);
		//private final IntWritable erro = new IntWritable(1);
		
		private Text word = new Text();
		private Text erro = new Text();
		
		public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
	
			//new
			try {
			if(value.toString().split(";").length == 4) {
			//new
			String[] line = value.toString().split(";");
			//new
			long x = Long.parseLong(line[0]);
        	int hour = LocalDateTime.ofEpochSecond(x/1000, 0, ZoneOffset.of("-02:00:00")).getHour();
        	length.set(hour);	
        	if (hour == 11) {
			//new
			Matcher matcher = TAG_PATTERN.matcher(line[2]);
            while (matcher.find()) {
            	String found = matcher.group();
          
            	String useMe = found;

                word.set(useMe.toLowerCase());
                context.write(word, ONE);
            }	
		}
		}
		} catch (NumberFormatException e){
        	String dummy = "erro";
			erro.set(dummy);
        	context.write(erro, ONE);
	}
		}
  }

	public static class LongSumReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
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
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "hashtag count");
		job.setJarByClass(HashtagCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}