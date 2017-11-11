import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetHashMapper extends
        Mapper<Object, Text, Text, LongWritable> {

    	final static Pattern TAG_PATTERN = Pattern.compile("#[a-zA-Z0-9_]+.");
		private final static LongWritable one = new LongWritable(1L);
		private final IntWritable length = new IntWritable(1);
		private Text word = new Text();
		private Text erro = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

			try {
			if(value.toString().split(";").length == 4) {

			String[] line = value.toString().split(";");
			
			long x = Long.parseLong(line[0]);
        	int hour = LocalDateTime.ofEpochSecond(x/1000, 0, ZoneOffset.of("-02:00:00")).getHour();
        	length.set(hour);	
        	if (hour == 23) {

			Matcher matcher = TAG_PATTERN.matcher(line[2]);
            while (matcher.find()) {
            	String found = matcher.group();
          
            	String useMe = found;

                word.set(useMe.toLowerCase());
                context.write(word, one);
            }	
		}
		}
		} catch (NumberFormatException e){
        	String dummy = "erro";
			erro.set(dummy);
        	context.write(erro, one);
	}
		}
  }



