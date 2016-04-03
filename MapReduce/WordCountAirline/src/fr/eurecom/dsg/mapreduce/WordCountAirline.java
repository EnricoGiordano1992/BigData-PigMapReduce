package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**********************************************************************************/
/**********************************************************************************/
/**********************************************************************************/


/****************************
 * 
 * WORDCOUNT FUNCTION
 *
 ***********************/

public class WordCountAirline extends Configured implements Tool {

  private int numReducers;
  private Path inputPath;
  private Path outputDir;

  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = this.getConf();
    Job job = new Job(conf, "Word count");

    job.setInputFormatClass(TextInputFormat.class);

    job.setMapperClass(WCMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(WCReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(TextOutputFormat.class);
   
    FileInputFormat.addInputPath(job, new Path(args[1]));

    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    job.setNumReduceTasks(Integer.parseInt(args[0]));

    job.setJarByClass(WordCountAirline.class);

    return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
  }
  
  public WordCountAirline (String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: WordCount <num_reducers> <input_path> <output_path>");
      System.exit(0);
    }
    this.numReducers = Integer.parseInt(args[0]);
    this.inputPath = new Path(args[1]);
    this.outputDir = new Path(args[2]);
  }
  
  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WordCountAirline(args), args);
    System.exit(res);
  }
}

/**********************************************************************************/
/**********************************************************************************/
/**********************************************************************************/


/****************************
 * 
 * MAPPER
 *
 ***********************/

class WCMapper extends Mapper<LongWritable, //input key type
                              Text, //input value type
                              Text, //output key type
                              IntWritable> { //output value type

	
	protected boolean isEmpty(String s){
		return (s != null && !s.isEmpty());
	}
	
	protected boolean isNumber(String s){
		return s.matches("-?\\d+(\\.\\d+)?");
	}
	
  @Override
  protected void map(LongWritable key, //input key type
                     Text value, //input value type
                     Context context) throws IOException, InterruptedException {

    //the map method (use context.write to emit results)
	  String[] words = value.toString().split(",");
	  String res;
	  //mouth 1, dcode 17
	  if(words.length > 17){
		  words[1].replaceAll("[,;\\s]", "");
		  words[17].replaceAll("[,;\\s]", "");
		  if(isEmpty(words[1]) && isEmpty(words[17]) && isNumber(words[1])){
			  res = words[1] + " " + words[17];
			  context.write(new Text(res), new IntWritable(1));
		  }
	  }
		  
/*
	  StringTokenizer str= new StringTokenizer(value.toString());
	  while(str.hasMoreTokens()){
		  StringTokenizer str_comma = new StringTokenizer(new Text(str.nextToken()), ',');
		  context.write(new Text(str.nextToken()), new IntWritable(1));

	  }
*/
  }

}

/**********************************************************************************/
/**********************************************************************************/
/**********************************************************************************/


/****************************
 * 
 * REDUCER
 *
 ***********************/

class WCReducer extends Reducer<Text, //input key type
                                IntWritable, //input value type
                                Text, //output key type
                                IntWritable> { //output value type

  @Override
  protected void reduce(Text key, //input key type
                        Iterable<IntWritable> values, //input value type
                        Context context) throws IOException, InterruptedException {

    //reduce method (use context.write to emit results)
	  int sum=0;
	  for(IntWritable v:values){
		  sum+=v.get();
	  }
	  context.write(key, new IntWritable(sum));
  }
}
