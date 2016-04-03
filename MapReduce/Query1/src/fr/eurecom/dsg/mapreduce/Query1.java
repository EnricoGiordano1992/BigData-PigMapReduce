package fr.eurecom.dsg.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/****************************
 * 
 * OGGETTO DI LAVORO
 *
 ***********************/

class CompositeKey implements WritableComparable {

	private String dcode;
	private String month;

	public CompositeKey() {
	}

	public CompositeKey(String udid, String datetime) {

		this.dcode = udid;
		this.month = datetime;
	}

	@Override
	public String toString() {

		return (new StringBuilder()).append(month).append(',').append(dcode).toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		dcode = WritableUtils.readString(in);
		month = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, dcode);
		WritableUtils.writeString(out, month);
	}

	public String getUDID() {

		return dcode;
	}

	public void setUDID(String udid) {

		this.dcode = udid;
	}

	public String getDatetime() {

		return month;
	}

	public void setDatetime(String datetime) {

		this.month = datetime;
	}

	@Override
	public int compareTo(Object o) {
		CompositeKey oComp = (CompositeKey) o;
		int result;
		Integer month1 = Integer.parseInt(month);
		Integer month2 = Integer.parseInt(oComp.month);		

		result = month1.compareTo(month2);
		if (0 == result) {
			result = dcode.compareTo(oComp.dcode);
		}
		return result;
	}

}

/*****************************************************************************************/
/*************************** JOB 1 *******************************************************/
/*****************************************************************************************/

/****************************
 * 
 * MAPPER
 *
 ***********************/

class WCAirplaneMapper extends Mapper<LongWritable, //input key type
Text, //input value type
CompositeKey, //output key type
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
		CompositeKey obj = new CompositeKey();

		String[] words = value.toString().split(",");
		String res;
		//mouth 1, dcode 17
		if(words.length > 17){
			words[1].replaceAll("[,;\\s]", "");
			words[17].replaceAll("[,;\\s]", "");
			if(isEmpty(words[1]) && isEmpty(words[17]) && isNumber(words[1])){
				obj.setDatetime(words[1]);
				obj.setUDID(words[17]);
				context.write(obj, new IntWritable(1));
			}
		}

	}

}

/****************************
 * 
 * REDUCER
 *
 ***********************/

class WCAirplaneReducer extends Reducer<CompositeKey, //input key type
IntWritable, //input value type
Text, //output key type
IntWritable> { //output value type

	@Override
	protected void reduce(CompositeKey obj, //input key type
			Iterable<IntWritable> values, //input value type
			Context context) throws IOException, InterruptedException {

		//reduce method (use context.write to emit results)
		int sum=0;
		for(IntWritable v:values){
			sum+=v.get();
		}
		context.write(new Text(obj.toString()), new IntWritable(sum));
	}
}

usa cleanup, in cui fai il sort di 3 Map<> variabile globale di reduce

/*****************************************************************************************/
/*************************** JOB 2 *******************************************************/
/*****************************************************************************************/

/****************************
 * 
 * MAPPER
 *
 ***********************/

class ReorderAirplainMapper extends Mapper<LongWritable, //input key type
Text, //input value type
CompositeKey, //output key type
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
		CompositeKey obj = new CompositeKey();

		String[] words = value.toString().split(",");
		String res;
		//mouth 1, dcode 17
		if(words.length > 17){
			words[1].replaceAll("[,;\\s]", "");
			words[17].replaceAll("[,;\\s]", "");
			if(isEmpty(words[1]) && isEmpty(words[17]) && isNumber(words[1])){
				obj.setDatetime(words[1]);
				obj.setUDID(words[17]);
				context.write(obj, new IntWritable(1));
			}
		}

	}

}

/****************************
 * 
 * REDUCER
 *
 ***********************/

class ReorderAirplaneReducer extends Reducer<CompositeKey, //input key type
IntWritable, //input value type
Text, //output key type
IntWritable> { //output value type

	@Override
	protected void reduce(CompositeKey obj, //input key type
			Iterable<IntWritable> values, //input value type
			Context context) throws IOException, InterruptedException {

		//reduce method (use context.write to emit results)
		int sum=0;
		for(IntWritable v:values){
			sum+=v.get();
		}
		context.write(new Text(obj.toString()), new IntWritable(sum));
	}
}


/**********************************************************************************/
/**********************************************************************************/
/**********************************************************************************/
/**********************************************************************************/
/**********************************************************************************/
/**********************************************************************************/

/****************************
 * 
 * QUERY DRIVER
 *
 ***********************/

public class Query1 extends Configured implements Tool {

	private int numReducers;
	private Path inputPath;
	private Path outputDir;
	private static final String OUTPUT_PATH = "intermediate_output";

	@Override
	public int run(String[] args) throws Exception {

		Job job1 = new Job(this.getConf(), "Keys count Airplane Q1");
		job1.setJarByClass(Query1.class);

		job1.setInputFormatClass(TextInputFormat.class);

		job1.setMapperClass(WCAirplaneMapper.class);
		job1.setMapOutputKeyClass(CompositeKey.class);
		job1.setMapOutputValueClass(IntWritable.class);

		job1.setReducerClass(WCAirplaneReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));

		job1.setNumReduceTasks(Integer.parseInt(args[0]));

		job1.waitForCompletion(true);

		/**************************************************************/
		
		Job job2 = new Job(this.getConf(), "TOP(20) Airplane Q1");
		job2.setJarByClass(Query1.class);

		job2.setMapperClass(ReorderAirplainMapper.class);
		job2.setReducerClass(ReorderAirplaneReducer.class);
		job2.setMapOutputValueClass(IntWritable.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
		TextOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.setNumReduceTasks(Integer.parseInt(args[0]));

		return job2.waitForCompletion(true) ? 0 : 1;		
	}

	public Query1 (String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: WordCount <num_reducers> <input_path> <output_path>");
			System.exit(0);
		}
		this.numReducers = Integer.parseInt(args[0]);
		this.inputPath = new Path(args[1]);
		this.outputDir = new Path(args[2]);
	}

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query1(args), args);
		System.exit(res);
	}
}

/**********************************************************************************/
/**********************************************************************************/
/**********************************************************************************/


