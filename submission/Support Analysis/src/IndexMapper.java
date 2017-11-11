public class indexMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
 
private final IntWritable one = new IntWritable(1);
 
public void map(Object, Text , Context context)
 
throws IOException,InterruptedException
 
{
 
if(value.toString().split(";").length == 4) {

	String[] line = value.toString().split(";");
	String tweet = line[1]
 
//Split the line in words
 
String words[]=tweet.split(" ");
 
for(String s:words){
 
//for each word emit word as key and file name as value
 
context.write(new Text(s), one);
 
}
 
}
 
}
}