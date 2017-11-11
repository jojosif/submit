import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GetNameMapper extends Mapper<Object, Text, Text, IntWritable> { 

	private final IntWritable one = new IntWritable(1);
    private Text name = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
      //look only if size = X 
      if(value.toString().split(",").length == 5) {
      				
      		String[] line = value.toString().split(";");
			name.set(line[1])
          	context.write(data, one);
        }
    
    }
}
