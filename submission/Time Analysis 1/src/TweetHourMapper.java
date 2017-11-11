import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class TweetHourMapper extends
        Mapper<Object, Text, IntWritable, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private final IntWritable length = new IntWritable(1);
    private final IntWritable erro = new IntWritable(1);

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

       
    	if(value.toString().split(";").length == 4) {

       try {    // get [0]
        	
        	String[] line = value.toString().split(";");
        	
        
        	long x = Long.parseLong(line[0]);
        	int hour = LocalDateTime.ofEpochSecond(x/1000, 0, ZoneOffset.of("-02:00:00")).getHour();
        	length.set(hour);
        	//length.set(line[2].length());
            //double number = line[2].length() ;
            //tir.set((int) (number / 5));
            
            
           // if (number < 141){
                context.write(length, one);


            //}
        }  catch (NumberFormatException e){
        	
        	erro.set(27);
        	context.write(erro, one);
        	
        
        } 
        }
    }
}



