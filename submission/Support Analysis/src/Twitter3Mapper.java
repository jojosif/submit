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

public class Twitter3Mapper extends Mapper<Object, Text, Text, IntWritable> { 

	private HashMap<String, String> countryCodes;
	
	 @Override
	  protected void setup(Context context) throws IOException, InterruptedException {
	    countryCodes = new HashMap<String, String>();
	    // We know there is only one cache file, so we only retrieve that URI
	    URI fileUri = context.getCacheFiles()[0];
	    FileSystem fs = FileSystem.get(context.getConfiguration());
	    FSDataInputStream in = fs.open( new Path(fileUri) );
	    BufferedReader br = new BufferedReader(new InputStreamReader(in));
	    String line = null;
	    try {
	    	
	      br.readLine();
	      while ((line = br.readLine()) != null) {
	    	  String [] arr= line.split(",");
	          
	    	      if(arr.length == 11){
	    	    	  String g = arr[1];
	    	    	 
	    	    	  if(g.length() != null){
	    	    		  String name = arr[1]
	    	    		  String sport = arr[8];

	    	 
	    	    		  countryCodes.put(name, sport);
	    	    		  
	    	    	  }
	    	      } 
	    	      }
	      br.close();
	     } catch (IOException e1) {
	     }
	   super.setup(context);
	   }

	 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	// Format per tweet is id;date;hashtags;tweet;
    	    String dump = value.toString();
    	    int startIndex;
    	    int endIndex;
    	      	    
    	    if(StringUtils.ordinalIndexOf(dump,";",4)>-1){
    	        startIndex = StringUtils.ordinalIndexOf(dump,";",2) + 1;
    	        endIndex = StringUtils.ordinalIndexOf(dump,";",3) + 1;
    	        String hashtags = dump.substring(startIndex, endIndex);
    	        Pattern p = Pattern.compile("(go|team)(\\w+)");
    	    	Matcher m = p.matcher(hashtags);
    	    	String countryCode;
    	    	
    	    	if(checkForAscii(hashtags)){
    	    		while (m.find()) {
    	    			countryCode = m.group(2);
    	    			if(countryCodes.containsKey(countryCode)){
    	    				data.set(countryCodes.get(countryCode));
    	    				context.write(data, one);
    	    				
    	    			}
    	    			
    	    		}
    	    	}
    	        
    	      }
    	}
    
    public static boolean checkForAscii(String tweet) {
    	for (char c: tweet.toCharArray()){
    		  if (((int)c)>127){
    		    return false;
    		  } 
    		}
    		return true;	 
    		    
    		}
}