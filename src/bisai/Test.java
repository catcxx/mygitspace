package bisai;


import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Test {

	// /step 1
	static int n1 = 0;
	static int i1 = 0;
	static int j1 = 0;
	static int k1 = 0;
	static int num1 = 0;
	static List<String> list1a = new ArrayList<String>();
	static List<String> list1b = new ArrayList<String>();

	public static class bisaiMap1 extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String hour = line.split(":")[1];
			String standardTime = SwitchStandTime(hour);

			int num = line.split(" ").length;
			
			if(num==10){//check data
			String statucode = line.split(" ")[7];

			if (statucode.equals("200") || statucode.equals("404")
					|| statucode.equals("500")) {
				n1++;
				context.write(new Text(standardTime), new Text(statucode));
				System.out.println(standardTime + " " + statucode);
			}
			}
		}
	}

	public static String SwitchStandTime(String str) {
		int time = Integer.parseInt(str.toString());
		String standardTime = "";
		if (time < 9) {
			standardTime = str + ":00-" + "0" + Integer.toString(time + 1)
					+ ":00";
		} else if (time == 9) {
			standardTime = str + ":00-" + Integer.toString(time + 1) + ":00";
		} else if (time == 23) {
			standardTime = str + ":00-00:00";
		} else {
			standardTime = str + ":00-" + Integer.toString(time + 1) + ":00";
		}
		return standardTime;
	}
	
	public static class bisaiReduce1 extends
			Reducer<Text, Text, Text, IntWritable> {
		String okey;
		// boolean flag=true;
		int m1 = 0;
		int sum1a = 0;
		int sum1b = 0;
		int sum1c = 0;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			m1++;
			// Text val = (Text) values;
			// System.out.println(m+"dddddddddddd"+key);
			for (Text val : values) {
				if (m1 == 1) {
					okey = key.toString().trim();
				}
//				System.out.println(okey + "sssssssssssssssssssss" + key);

				String v = val.toString().trim();
				if (key.toString().trim().equals(okey)) {

					if (v.equals("200")) {
						i1++;
						num1++;
						sum1a++;
					}
					if (v.equals("404")) {
						j1++;
						num1++;
						sum1b++;
					}
					if (v.equals("500")) {
						k1++;
						num1++;
						sum1c++;
					}

					// System.out.println(num1+"aabbbbbbbbba");

				} else {
					list1a.add(okey + " " + "200:" + i1 + " 404:" + j1
							+ " 500:");
					list1b.add(k1 + "");
					okey = key.toString();
					i1 = j1 = k1 = 0;
					if (v.equals("200")) {
						i1++;
						num1++;
						sum1a++;
					}
					if (v.equals("404")) {
						j1++;
						num1++;
						sum1b++;
					}
					if (v.equals("500")) {
						k1++;
						num1++;
						sum1c++;
					}
					// System.out.println(list.size()+"aaavvvvvvvvvvvvvvvvvaaaaaa"+n);

					// flag=false;
				}
				// System.out.println(list.size()+"vvvvvvaaaaaa"+n);
				if (num1 == n1) {
					list1a.add(okey + " " + "200:" + i1 + " 404:" + j1
							+ " 500:");
					list1b.add(k1 + "");
					context.write(new Text("200:"), new IntWritable(sum1a));
					context.write(new Text("404:"), new IntWritable(sum1b));
					context.write(new Text("500:"), new IntWritable(sum1c));
					for (int ab = 0; ab < list1a.size(); ab++) {
						context.write(
								new Text(list1a.get(ab)),
								new IntWritable(
										Integer.parseInt(list1b.get(ab))));
					}

				}
			}

		}
	}

	public static class txtOutputFormat1 extends
			MultipleOutputFormat1<Text, IntWritable> {
		@Override
		protected String generateFileNameForKeyValue(Text key,
				IntWritable value, Configuration conf) {
			// char c = key.toString().toLowerCase().charAt(0);
			// if (c >= 'a' && c <= 'z') {
			String c = key.toString();
			// return c + ".txt";
			// }
			return "1.txt";
		}
	}

	// //////////////step 2
	static int sum2a = 0;
	static int sum2b = 0;
	static List<String> listnum2 = new ArrayList<String>();

	public static class bisaiMap2 extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String ip = line.split(" ")[0];
			String time1 = line.split(":")[1];
			int time2 = (Integer.parseInt(time1) + 1);
			String time = time1 + ":00-" + time2 + ":00";
			if (!(ip.trim().equals("-"))) {
				context.write(new Text(ip + ";" + time), one);
			}
		}
	}

	public static class bisaiReduce2 extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		int n = 0;
		String okey;
		static List<String> listkey = new ArrayList<String>();
		static List<IntWritable> listresult = new ArrayList<IntWritable>();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			String key1 = key.toString().split(";")[0];// ip
			n++;
			if (n == 1) {
				okey = key1;
			}
			String time = key.toString().split(";")[1];// 00:00-1:00

			for (IntWritable val : values) {
				sum2a += val.get();
			}

			if (key1.equals(okey)) {
				sum2b = sum2a + sum2b;
			} else {
				if (sum2b != 0) {
					context.write(new Text(" " + okey), new IntWritable(sum2b));
				}
				for (int i = 0; i < listresult.size(); i++) {
					context.write(new Text(listkey.get(i)), listresult.get(i));
				}
				listkey.clear();
				listresult.clear();
				okey = key1;
				sum2b = sum2a;
			}
			listkey.add(time + " " + key1);
			listresult.add(new IntWritable(sum2a));
			sum2a = 0;

		}
	}

	public static class txtOutputFormat2 extends
			MultipleOutputFormat<Text, IntWritable> {
		@Override
		protected String generateFileNameForKeyValue(Text key,
				IntWritable value, Configuration conf) {
			// char c = key.toString().toLowerCase().charAt(0);
			// if (c >= 'a' && c <= 'z') {
			// String c = key.toString();
			// return c + ".txt";
			// }
			String c = key.toString().split(" ")[1];
			return c + ".txt";
		}
	}

	// ///////////step 3
	static int sum3a = 0;
	static int sum3b = 0;
	static List<String> listnum3 = new ArrayList<String>();

	public static class bisaiMap3 extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String interf = line.split(" ")[4];
			String hour = line.split(":")[1].split(" ")[0];
			String min = line.split(":")[2].split(" ")[0];

			String sec = line.split(":")[3].split(" ")[0];

			String time = hour + ":" + min + ":" + sec;

			if (!(interf.trim().equals("null"))) {
				context.write(new Text(interf + ";" + time), one);
				// System.out.println(interf + ";" + time);
			}

		}
	}

	public static class bisaiReduce3 extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		int n = 0;
		String okey;
		static List<String> listkey = new ArrayList<String>();
		static List<IntWritable> listresult = new ArrayList<IntWritable>();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			String key1 = key.toString().split(";")[0];// interface

			n++;
			if (n == 1) {
				okey = key1;
			}

			String time = key.toString().split(";")[1];
			for (IntWritable val : values) {
				sum3a += val.get();
			}

			if (!(key1.equals(okey))) {
				// context.write(new Text(" " + okey), new IntWritable(sum2));
				if (sum3b != 0) {
					context.write(new Text(" " + okey), new IntWritable(sum3b));
				}
				for (int i = 0; i < listresult.size(); i++) {
					context.write(new Text(listkey.get(i)), listresult.get(i));
				}
				listkey.clear();
				listresult.clear();
				okey = key1;
				sum3b = sum3a;
			} else {
				sum3b = sum3a + sum3b;
			}

			// context.write(new Text(time + " " + key1), new IntWritable(sum));
			listkey.add(time + " " + key1);
			listresult.add(new IntWritable(sum3a));
			sum3a = 0;
		}
	}

	public static class txtOutputFormat3 extends
			MultipleOutputFormat<Text, IntWritable> {
		@Override
		protected String generateFileNameForKeyValue(Text key,
				IntWritable value, Configuration conf) {

			String c = key.toString().split(" ")[1];
			String key2 = c.replaceAll("/", "-");
			String key3 = key2.replaceFirst("-", "");
			return key3 + ".txt";
		}
	}

	static int sum4a = 0;
	static int countall4 = 0;
	static int sum4b = 0;
	static List<String> listnum = new ArrayList<String>();

	public static class resptimetest extends
			Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String interf = line.split(" ")[4];
			String time1 = line.split(":")[1];
			int time2 = (Integer.parseInt(time1) + 1);
			String time = time1 + ":00-" + time2 + ":00";
			int resptime = Integer.parseInt(line.split(" ")[9]);
			// System.out.println(interf+";"+time+";"+resptime);
			if (!(interf.trim().equals("null"))) {
				context.write(new Text(interf + ";" + time), new IntWritable(
						resptime));

			}
		}
	}

	public static class resptimereduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		int n = 0;
		String okey;
		static List<String> listkey = new ArrayList<String>();
		static List<IntWritable> listresult = new ArrayList<IntWritable>();
		static int count = 0;

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			String key1 = key.toString().split(";")[0];// interface
			n++;
			if (n == 1) {
				okey = key1;
			}
			String time = key.toString().split(";")[1];// time

			for (IntWritable val : values) {
				sum4a += val.get();
				count++;
			}
			// System.out.println(sum+" "+count);

			// System.out.println(n);
			if (!(key1.equals(okey))) {
				if (sum4b != 0) {
					context.write(new Text(" " + okey), new IntWritable(sum4b));
				}
				for (int i = 0; i < listresult.size(); i++) {
					context.write(new Text(listkey.get(i)), listresult.get(i));
				}
				listkey.clear();
				listresult.clear();
				okey = key1;
				sum4b = sum4a;
			} else {
				sum4b = sum4a + sum4b;
			}

			listkey.add(time + " " + key1);
			listresult.add(new IntWritable(sum4a / count));
			sum4a = 0;
			count = 0;
		}
	}

	public static class txtOutputFormat4 extends
			MultipleOutputFormat<Text, IntWritable> {
		@Override
		protected String generateFileNameForKeyValue(Text key,
				IntWritable value, Configuration conf) {

			String c = key.toString().split(" ")[1];
			String key2 = c.replaceAll("/", "-");
			String key3 = key2.replaceFirst("-", "");
			return key3 + ".txt";
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();

		String[] otherArgs1 = new GenericOptionsParser(conf1, args)
				.getRemainingArgs();
		if (otherArgs1.length != 5) {
			System.err
					.println("Usage: <args0--input> <args1--out1> <args2--out2><args3--out3> <args4--out4>");
			System.exit(2);
		}

		Job job1 = new Job(conf1);
		job1.setJarByClass(Test.class);
		job1.setJobName("bisai1");
		final FileSystem filesystem1 = FileSystem.get(new URI(args[1]), conf1);
		if (filesystem1.exists(new Path(args[1]))) {
			filesystem1.delete(new Path(args[1]), true);
		}
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(bisaiMap1.class);
		// job.setCombinerClass(bidaitestReduce.class);
		job1.setReducerClass(bisaiReduce1.class);
		final FileSystem filesystem = FileSystem.get(new URI(args[1]), conf1);
		if (filesystem.exists(new Path(args[1]))) {
			filesystem.delete(new Path(args[1]), true);
		}
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(txtOutputFormat1.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2);
		job2.setJarByClass(Test.class);
		job2.setJobName("bisai2");
		final FileSystem filesystem2 = FileSystem.get(new URI(args[2]), conf2);
		if (filesystem2.exists(new Path(args[2]))) {
			filesystem2.delete(new Path(args[2]), true);
		}
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setMapperClass(bisaiMap2.class);
		// job.setCombinerClass(bidaitestReduce.class);
		job2.setReducerClass(bisaiReduce2.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(txtOutputFormat2.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(true);

		Configuration conf3 = new Configuration();
		Job job3 = new Job(conf3);
		job3.setJarByClass(Test.class);
		job3.setJobName("bisai3");
		final FileSystem filesystem3 = FileSystem.get(new URI(args[3]), conf3);
		if (filesystem3.exists(new Path(args[3]))) {
			filesystem3.delete(new Path(args[3]), true);
		}
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		job3.setMapperClass(bisaiMap3.class);
		job3.setReducerClass(bisaiReduce3.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(txtOutputFormat3.class);
		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		job3.waitForCompletion(true);

		Configuration conf4 = new Configuration();
		Job job4 = new Job(conf4);
		job4.setJarByClass(Test.class);
		job4.setJobName("bisai4");
		final FileSystem filesystem4 = FileSystem.get(new URI(args[4]), conf4);
		if (filesystem4.exists(new Path(args[4]))) {
			filesystem4.delete(new Path(args[4]), true);
		}
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(IntWritable.class);
		job4.setMapperClass(resptimetest.class);
		job4.setReducerClass(resptimereduce.class);
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(txtOutputFormat4.class);
		FileInputFormat.addInputPath(job4, new Path(args[0]));
		FileOutputFormat.setOutputPath(job4, new Path(args[4]));
		job4.waitForCompletion(true);

	}

}

