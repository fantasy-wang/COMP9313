package Assignment1;
/* For this assignment, firstly, I think about using two mapper and two reducer to do this assignment. 
 * For this first mapper, I split the lines by split the "::" and the get the user, movie and rating
 * I use user as a key, movie and rating as value. 
 * For this step, I define a class which implements Writable to store movie and rating and override the toString function. 
 * Then, I did the first reducer, for the same key user, 
 * I define a class UArrayWritable implementing ArrayWritable to store the a list of movie and rating and override toString() function.
 * Then, I use job input path and output path to chain the two mapreduce.
 * For the second mapreduce, I split the lines, first split by "\t" to get user and the string of movie and rating
 * Next, I split the "," to get movie and rating.
 * I define a classUmKeys implementing WritableComparable to store the movie_1 and movie_2 as key,
 * in this class, I override toString() function and CompaerTo() function.
 * Then, I also define a class UmValues implementing Writable to store user, rating1 and rating2 as value,
 * in this class, I override the toString() function.
 * Then, for second reducer, I also use UmKeys as key which is movie_1 and movie_2.
 * Next, I define a class UmArrayWritable implementing ArrayWritable to shore a list of user, rating1 and rating2 and voerride toString() function.
 * Finally, I define the output path and check it.
*/



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class AssigOne{
	
	//define a class implements Writable to store movie and rating
	public static class MR_Writable implements Writable{
		private Text movie;
		private IntWritable rating;
		
		public MR_Writable(){
			movie = new Text("");
			rating = new IntWritable(-1);
		}
		
		public MR_Writable(Text movie, IntWritable rating){
			this.movie = movie;
			this.rating = rating;
		}

		public Text getMovie() {
			return movie;
		}

		public void setMovie(Text movie) {
			this.movie = movie;
		}

		public IntWritable getRating() {
			return rating;
		}

		public void setRating(IntWritable rating) {
			this.rating = rating;
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			this.movie.readFields(data);
			this.rating.readFields(data);
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.movie.write(data);
			this.rating.write(data);
		}

		@Override
		public String toString() {
			return this.movie.toString()+","+this.rating.toString();
		}
	}
	
	//mapper1: read input and save key:user and value:movie,rating
	public static class UserMovieMapper1 extends Mapper<LongWritable, Text, Text, MR_Writable>{
		
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MR_Writable>.Context context)
				throws IOException, InterruptedException {
		
			//get a line and split by "::"
			String line = value.toString();
			String[] lines = line.split("::");
			
			//ArrayWritable v = new ArrayWritable(Text.class);
			MR_Writable v = new MR_Writable();
			
			//get movie_id and rating and save
			Text movie_id = new Text(lines[1]);
			IntWritable rating = new IntWritable(Integer.parseInt(lines[2]));
			
			//use v to save movie_id and rating
			v.setMovie(movie_id);
			v.setRating(rating);
						
			//write out
			context.write(new Text(lines[0]), v);
		}
	}

	//define a class implementing to store list of movie and rating
	public static class UArrayWritable extends ArrayWritable {

		public UArrayWritable(Class<? extends Writable> valueClass) {
			super(valueClass);
			// TODO Auto-generated constructor stub
		}

		@Override
		public String toString() {
			String[] strings = this.toStrings();
			return String.join(" ", strings);
		}
	}
	
	//reducer1: read output from mapper1 and, for same key, use string to save values, like U	M,R M,R...etc
	public static class UserMovieReducer1 extends Reducer<Text, MR_Writable, Text, UArrayWritable>{

		@Override
		protected void reduce(Text key, Iterable<MR_Writable> values,
				Reducer<Text, MR_Writable, Text, UArrayWritable>.Context context)
				throws IOException, InterruptedException {	
			
			//define value
			UArrayWritable uArrayWritable = new UArrayWritable(MR_Writable.class);
			
			//define list to store values
			ArrayList<MR_Writable> list = new ArrayList<MR_Writable>();
			for (MR_Writable v : values) {
				list.add(new MR_Writable(new Text(v.getMovie().toString()),new IntWritable(Integer.parseInt(v.getRating().toString()))));
			}
			
			//let list change to array and use v to store uvalues
			MR_Writable[] mr = list.toArray(new MR_Writable[list.size()]);
			uArrayWritable.set(mr);
			
			context.write(key, uArrayWritable);			
		}
	}
	
	//UmKeys: define a class implements WritableComparable to store movie_1 and movie_2
	public static class UmKeys implements WritableComparable<UmKeys>{
		private Text movie_1;
		private Text movie_2;
		
		public UmKeys() {
			this.movie_1 = new Text("");
			this.movie_2 = new Text("");
		}
		
		public UmKeys(Text movie_1, Text movie_2) {
			this.movie_1 = movie_1;
			this.movie_2 = movie_2;
		}
		
		public Text getMovie_1() {
			return movie_1;
		}
		public void setMovie_1(Text movie_1) {
			this.movie_1 = movie_1;
		}
		public Text getMovie_2() {
			return movie_2;
		}
		public void setMovie_2(Text movie_2) {
			this.movie_2 = movie_2;
		}
		@Override
		public void readFields(DataInput input) throws IOException {
			this.movie_1.readFields(input);
			this.movie_2.readFields(input);
		}
		@Override
		public void write(DataOutput output) throws IOException {
			this.movie_1.write(output);
			this.movie_2.write(output);
		}
		@Override
		public int compareTo(UmKeys o) {
			int cmp = movie_1.compareTo(((UmKeys) o).movie_1);
			if (cmp != 0) {
				return cmp;
			}
			return movie_2.compareTo(((UmKeys) o).movie_2);
		}

		@Override
		public String toString() {
			return "("+this.movie_1+","+this.movie_2+")";
		}
	}
	
//	//UmValues: define a class implements Writable to store user,rating1,rating2
	public static class UmValues implements Writable{
		private Text user;
		private IntWritable rating1;
		private IntWritable rating2;
		
		public UmValues() {
			this.user = new Text("");
			this.rating1 = new IntWritable(-1);
			this.rating2 = new IntWritable(-1);
		}
		
		public UmValues(Text user, IntWritable rating1, IntWritable rating2) {
			this.user = user;
			this.rating1 = rating1;
			this.rating2 = rating2;
		}
		
		public Text getUser() {
			return user;
		}

		public void setUser(Text user) {
			this.user = user;
		}

		public IntWritable getRating1() {
			return rating1;
		}

		public void setRating1(IntWritable rating1) {
			this.rating1 = rating1;
		}

		public IntWritable getRating2() {
			return rating2;
		}

		public void setRating2(IntWritable rating2) {
			this.rating2 = rating2;
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			this.user.readFields(data);
			this.rating1.readFields(data);
			this.rating2.readFields(data);
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.user.write(data);
			this.rating1.write(data);
			this.rating2.write(data);
		}

		@Override
		public String toString() {
			return "("+this.user+","+this.rating1+","+this.rating2+")";
		}
	}
	
	//mapper2: read input from reducer1 and put two M into UmKeys and put U and two rating into UmValues
	public static class UserMovieMapper2 extends Mapper<Object, Text, UmKeys, UmValues>{

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, UmKeys, UmValues>.Context context)
				throws IOException, InterruptedException {
			
			//create key and value
			UmKeys k = new UmKeys();
			UmValues v = new UmValues();
			
			
			//split values
			String[] val = value.toString().split("\t");
			String user = val[0];
			String other = val[1];
			String[] va = other.split(" ");
			
			
			
			//loop every two elements and save them into keys and values
			for (int i=0;i<va.length;i++) {
				for (int j=i+1;j<va.length;j++) {
					String[] vi = va[i].split(",");
					String[] vj = va[j].split(",");
					
					k.setMovie_1(new Text(vi[0]));
					k.setMovie_2(new Text(vj[0]));
					
					v.setUser(new Text(user));
					v.setRating1(new IntWritable(Integer.parseInt(vi[1])));
					v.setRating2(new IntWritable(Integer.parseInt(vj[1])));
					
					context.write(k, v);

				}
			}
		}
	}

	//define UmArrayWritable implementing ArrayWritable
	public static class UmArrayWritable extends ArrayWritable {

		public UmArrayWritable(Class<? extends Writable> valueClass) {
			super(valueClass);
			// TODO Auto-generated constructor stub
		}

		@Override
		public String toString() {
			String[] strings = this.toStrings();
			return "["+String.join(",", strings)+"]";
		}
	}
	
	
	//reducer2 for same key M1,M2, save U,R1,R2..... into string
	public static class UserMovieReducer2 extends Reducer<UmKeys, UmValues, UmKeys, UmArrayWritable>{

		@Override
		protected void reduce(UmKeys key, Iterable<UmValues> values,
				Reducer<UmKeys, UmValues, UmKeys, UmArrayWritable>.Context context) throws IOException, InterruptedException {
			
			//define value
			UmArrayWritable umArrayWritable = new UmArrayWritable(UmValues.class);
			
			//define list to store values
			ArrayList<UmValues> alist = new ArrayList<UmValues>();
			for (UmValues v : values) {
				alist.add(new UmValues(new Text(v.getUser().toString()), new IntWritable(Integer.parseInt(v.getRating1().toString())), new IntWritable(Integer.parseInt(v.getRating2().toString()))));
			}
			
			//let list change to array and use v to store uvalues
			UmValues[] uvalues = alist.toArray(new UmValues[alist.size()]);
			umArrayWritable.set(uvalues);
			
			
			context.write(key, umArrayWritable);
		}
	}
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		//job1
		//get job object
		Job job1 = Job.getInstance(conf);
		
		//2.set jar store path
		job1.setJarByClass(AssigOne.class);
		
		//connect map and reduce type
		job1.setMapperClass(UserMovieMapper1.class);
		job1.setReducerClass(UserMovieReducer1.class);
		
		//set mapper's output key and value type
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(MR_Writable.class);
		
		//set final output key and value type
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(UArrayWritable.class);
		
		//set input and output path
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1], "out1"));
		
		//submit job
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}
		
		//job2
		//get job object
		Job job2 = Job.getInstance(conf);
		
		//2.set jar store path
		job2.setJarByClass(AssigOne.class);
		
		//connect map and reduce type
		job2.setMapperClass(UserMovieMapper2.class);
		job2.setReducerClass(UserMovieReducer2.class);
		
		//set mapper's output key and value type
		job2.setMapOutputKeyClass(UmKeys.class);
		job2.setMapOutputValueClass(UmValues.class);
		
		//set final output key and value type
		job2.setOutputKeyClass(UmKeys.class);
		job2.setOutputValueClass(UmArrayWritable.class);
		
		//set input and output path
		FileInputFormat.addInputPath(job2, new Path(args[1], "out1"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1], "out2"));
		
		//submit job
		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}
