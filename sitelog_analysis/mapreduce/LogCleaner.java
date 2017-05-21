package mapreduce;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LogCleaner{
	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		LogParser logParser = new LogParser();
		Text v2 = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			final String[] parsed = logParser.parse(value.toString());
			
			//过滤掉静态信息
			if(parsed[2].startsWith("GET /static/") || parsed[2].startsWith("GET /uc_server")){
				return;
			}			
			//过掉开头的特定格式字符串
			if(parsed[2].startsWith("GET /")){
				parsed[2] = parsed[2].substring("GET /".length());
			}
			else if(parsed[2].startsWith("POST /")){
				parsed[2] = parsed[2].substring("POST /".length());
			}			
			//过滤结尾的特定格式字符串
			if(parsed[2].endsWith(" HTTP/1.1")){
				parsed[2] = parsed[2].substring(0, parsed[2].length()-" HTTP/1.1".length());
			}			
			v2.set(parsed[0]+"\t"+parsed[1]+"\t"+parsed[2]);
			context.write(key, v2);
		}
		
	}
	static class MyReducer extends Reducer<LongWritable, Text, Text, NullWritable>{
		@Override
		protected void reduce(LongWritable arg0, Iterable<Text> arg1,
				Reducer<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			for (Text v2 : arg1) {				
				context.write(v2, NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(LogParser.class);		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}