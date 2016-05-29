package fr.eurecom.dsg.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/****************************
 * 
 * OGGETTI DI LAVORO
 *
 ***********************/

@SuppressWarnings("rawtypes")
class CompositeKey implements WritableComparable {

	private String scode;
	private String dcode;

	public CompositeKey() {
	}

	public CompositeKey(String scode, String dcode) {
		this.scode = scode;
		this.dcode = dcode;
	}

	public CompositeKey(CompositeKey c){
		this.scode = c.scode;
		this.dcode = c.dcode;
	}

	@Override
	public String toString() {
		return (new StringBuilder()).append('(').append(scode).append(',').append(dcode).append(')').toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		scode = WritableUtils.readString(in);
		dcode = WritableUtils.readString(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, scode);
		WritableUtils.writeString(out, dcode);
	}

	public String getScode() {

		return scode;
	}

	public void setScode(String scode) {

		this.scode = scode;
	}

	public String getDcode() {

		return dcode;
	}

	public void setDcode(String dcode) {

		this.dcode = dcode;
	}

	@Override
	public int compareTo(Object o) {
		CompositeKey oComp = (CompositeKey) o;
		int result;

		result = scode.compareTo(oComp.scode);
		if (0 == result) {
			result = dcode.compareTo(oComp.dcode);
		}
		return result;
	}

	@Override
	public boolean equals(Object o) {
		CompositeKey temp = (CompositeKey) o;

		if (this == temp) 
			return true;
		else if (temp == null || getClass() != temp.getClass()) 
			return false;
		else{
			if(this.scode == temp.scode && this.dcode == temp.dcode)
				return true;
			else
				return false;
		}
	}

	@Override
	public int hashCode(){
		String builder = "";
		builder += this.scode;
		builder += this.dcode;
		return builder.hashCode();
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

	protected boolean isNotEmpty(String s){
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
		CompositeKey obj1 = new CompositeKey();

		String[] words = value.toString().split(",");
		//scode 16, dcode 17
		if(words.length > 17){
			words[16].replaceAll("[,;\\s]", "");
			words[17].replaceAll("[,;\\s]", "");

			if(isNotEmpty(words[16]) && isNotEmpty(words[17])){
				obj1.setScode(words[16]);
				obj1.setDcode(words[17]);
				context.write(obj1, new IntWritable(1));
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

public class Query5 extends Configured implements Tool {

	private int numReducers;
	private Path inputPath;
	private Path outputDir;

	@Override
	public int run(String[] args) throws Exception {

		Job job1 = new Job(this.getConf(), "Keys count Airplane Q2");
		job1.setJarByClass(Query5.class);
		job1.setNumReduceTasks(numReducers);

		job1.setInputFormatClass(TextInputFormat.class);

		job1.setMapperClass(WCAirplaneMapper.class);
		job1.setMapOutputKeyClass(CompositeKey.class);
		job1.setMapOutputValueClass(IntWritable.class);

		job1.setReducerClass(WCAirplaneReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, inputPath);
		FileOutputFormat.setOutputPath(job1, outputDir);

		job1.setNumReduceTasks(Integer.parseInt(args[0]));

		/**************************************************************/


		return job1.waitForCompletion(true) ? 0 : 1;		
	}

	public Query5 (String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: WordCount <num_reducers> <input_path> <output_path>");
			System.exit(0);
		}
		this.numReducers = Integer.parseInt(args[0]);
		this.inputPath = new Path(args[1]);
		this.outputDir = new Path(args[2]);
	}

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query5(args), args);
		System.exit(res);
	}
}

/**********************************************************************************/
/**********************************************************************************/
/**********************************************************************************/


