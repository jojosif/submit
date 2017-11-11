import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class Twitter3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
    ArrayList<String> vals= new ArrayList<String>();
    protected void setup(Context context) throws IOException, InterruptedException {
    URI fileUri = context.getCacheFiles()[0];
    FileSystem fs = FileSystem.get(context.getConfiguration());
    FSDataInputStream in = fs.open( new Path(fileUri) );
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String line = null;
    try {
    	
      br.readLine();
      while ((line = br.readLine()) != null) {
    	  String [] arr= line.split(" ");
    	  vals.add(line);
    	  
      }
      br.close();
     } catch (IOException e1) {
     }
   super.setup(context);
   }

    public void reduce(Text key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
        	sum += value.get();
  
        }
               result.set(sum);
               
               context.write(new Text(vals.toString()), result);

    }

}