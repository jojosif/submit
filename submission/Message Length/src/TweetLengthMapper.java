import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
