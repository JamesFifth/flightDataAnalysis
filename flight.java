package flight;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class flight {

	// Mapper One
	public static class MapperOne extends Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String[] line = value.toString().split(",");
			if (line[14].trim().matches("-?[0-9]+")) {
				int delay = Integer.valueOf(line[14]);
				if (delay >= -5 && delay <= 5) {
					context.write(new Text(line[8].trim() + line[9].trim()), new Text("1"));
				} else {
					context.write(new Text(line[8].trim() + line[9].trim()), new Text("0"));
				}
			}
		}

	}

	// Reducer One
	public static class ReducerOne extends Reducer<Text, Text, Text, Text> {

		LinkedList<Flight> hightest_on_schedule = new LinkedList<>();
		LinkedList<Flight> lowest_on_schedule = new LinkedList<>();

		class Flight implements Comparable<Flight> {
			String fNo;
			double onTime;

			public Flight(String relative, double percentage) {
				this.fNo = relative;
				this.onTime = percentage;
			}

			@Override
			public int compareTo(Flight flight) {
				if (this.onTime >= flight.onTime) {
					return -1;
				} else {
					return 1;
				}
			}
		}

		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int total = 0;
			int onTime = 0;
			for (Text value : values) {
				total++;
				if (value.toString().equals("1"))
					onTime++;
			}
			double perctg = (double) onTime / total * 100;
			Flight mFlight = new Flight(key.toString(), perctg);
			addToTop3onTime(mFlight);
			addToTop3Delay(mFlight);
		}

		private void addToTop3onTime(Flight f) {
			if (hightest_on_schedule.size() < 3)
				hightest_on_schedule.add(f);
			else {
				hightest_on_schedule.add(f);
				Collections.sort(hightest_on_schedule);
				hightest_on_schedule.removeLast();
			}
		}

		private void addToTop3Delay(Flight f) {
			if (lowest_on_schedule.size() < 3)
				lowest_on_schedule.add(f);
			else {
				lowest_on_schedule.add(f);
				Collections.sort(lowest_on_schedule);
				Collections.reverse(lowest_on_schedule);
				lowest_on_schedule.removeLast();
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("Three airlines with Highest probability for being on schedule:"), new Text(""));
			for (Flight f : hightest_on_schedule) {
				context.write(new Text(f.fNo), new Text(String.valueOf(f.onTime) + "%"));
			}
			context.write(new Text("Three airlines with lowest probability for being on schedule:"), new Text(""));
			for (Flight f : lowest_on_schedule) {
				context.write(new Text(f.fNo), new Text(String.valueOf(f.onTime) + "%"));
			}
		}
	}

	// Mapper Two
	public static class MapperTwo extends Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] line = value.toString().split(",");
			if (line[19].trim().matches("-?[0-9]+") && !line[19].trim().equals("")) {
				context.write(new Text(line[17]), new Text(line[19]));
			}
			if (line[20].trim().matches("-?[0-9]+") && !line[20].trim().equals("")) {
				context.write(new Text(line[16]), new Text(line[20]));
			}
		}
	}

	// Reducer Two
	public static class ReducerTwo extends Reducer<Text, Text, Text, Text> {
		LinkedList<Airport> longest_taxi_flight = new LinkedList<>();
		LinkedList<Airport> shortest_taxi_flight = new LinkedList<>();

		class Airport implements Comparable<Airport> {
			String airport;
			double aveTaxi;

			public Airport(String str, double av) {
				this.airport = str;
				this.aveTaxi = av;
			}

			@Override
			public int compareTo(Airport apt) {
				if (this.aveTaxi >= apt.aveTaxi) {
					return -1;
				} else {
					return 1;
				}
			}

		}

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double totalTime = 0;
			long number = 0;
			for (Text t : values) {
				number++;
				totalTime = +Math.abs(Integer.valueOf(t.toString()));
			}
			if (number > 0) {
				double avg = (double) totalTime / number;
				Airport mF = new Airport(key.toString(), avg);
				addLongestTaxiFlight(mF);
				addShortestTaxiFlight(mF);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("Three airports with the longest average taxi time per flight:"), new Text(""));
			for (Airport f : longest_taxi_flight) {
				context.write(new Text(f.airport), new Text(String.valueOf(f.aveTaxi)));
			}
			context.write(new Text("Three airports with the shortest average taxi time per flight:"), new Text(""));
			for (Airport f : shortest_taxi_flight) {
				context.write(new Text(f.airport), new Text(String.valueOf(f.aveTaxi)));
			}
		}

		private void addLongestTaxiFlight(Airport f) {
			if (longest_taxi_flight.size() < 3)
				longest_taxi_flight.add(f);
			else {
				longest_taxi_flight.add(f);
				Collections.sort(longest_taxi_flight);
				longest_taxi_flight.removeLast();
			}
		}

		private void addShortestTaxiFlight(Airport f) {
			if (shortest_taxi_flight.size() < 3)
				shortest_taxi_flight.add(f);
			else {
				shortest_taxi_flight.add(f);
				Collections.sort(shortest_taxi_flight);
				Collections.reverse(shortest_taxi_flight);
				shortest_taxi_flight.removeLast();
			}
		}
	}

	// Mapper Three
	public static class MapperThree extends Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] line = value.toString().split(",");
			String cancelCode = line[22].trim();
			if (line[0] != null && !line[0].trim().equals("Year")) {
				if (cancelCode != null && !cancelCode.equals("") && !cancelCode.equals("NA")) {
					context.write(new Text(cancelCode), new Text("1"));
				}
			}
		}
	}

	// Reducer Three
	public static class ReducerThree extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		int max = 0;
		Text maxCode = new Text();

		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (Text v : values) {
				sum += Integer.valueOf(v.toString());
			}

			if (sum >= max) {
				maxCode.set(key);
				max = sum;
			}

			result.set(String.valueOf(sum));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("The most common reason for flight cancellations:"), new Text());
			context.write(maxCode, new Text(String.valueOf(max)));
		}

	}

// driver class main
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Job1");
		job1.setJarByClass(flight.class);
		job1.setMapperClass(MapperOne.class);
		job1.setReducerClass(ReducerOne.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setNumReduceTasks(1);
		FileSystem fs = FileSystem.get(conf);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		if (fs.exists(new Path(args[1] + "_FlightsOnSchedule"))) {
			fs.delete(new Path(args[1] + "_FlightsOnSchedule"), true);
		}
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_FlightsOnSchedule"));
		if (job1.waitForCompletion(true)) {
			Job job2 = Job.getInstance(conf, "Job2");
			job2.setJarByClass(flight.class);
			job2.setMapperClass(MapperTwo.class);
			job2.setReducerClass(ReducerTwo.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setNumReduceTasks(1);
			FileInputFormat.addInputPath(job2, new Path(args[0]));
			if (fs.exists(new Path(args[1] + "_TaxiInTime"))) {
				fs.delete(new Path(args[1] + "_TaxiInTime"), true);
			}
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_TaxiInTime"));
			if (job2.waitForCompletion(true)) {
				Job job3 = Job.getInstance(conf, "Job3");
				job3.setJarByClass(flight.class);
				job3.setMapperClass(MapperThree.class);
				job3.setReducerClass(ReducerThree.class);
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);
				job3.setNumReduceTasks(1);
				FileInputFormat.addInputPath(job3, new Path(args[0]));
				if (fs.exists(new Path(args[1] + "_FlightCancellation"))) {
					fs.delete(new Path(args[1] + "_FlightCancellation"), true);
				}
				FileOutputFormat.setOutputPath(job3, new Path(args[1] + "_FlightCancellation"));
				System.exit(job3.waitForCompletion(true) ? 0 : 1);
			}
		}
	}
}
