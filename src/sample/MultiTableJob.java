package org.hadoop.extra;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;

import com.brilig.hadoop.users.matching.IpMatchingMappper;
import com.brilig.hadoop.users.matching.IpMatchingJob.IpGroup;

public class MultiTableJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());

		job.setJarByClass(MultiTableJob.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(MultiTableInputFormat.class); // <--------------
		job.setOutputFormatClass(NullOutputFormat.class);

		job.setMapperClass(IpMatchingMappper.class);
		job.setReducerClass(IpGroup.class);

		Configuration conf1 = new Configuration();
		Configuration conf2 = new Configuration();

		conf1.set(TableInputFormat.INPUT_TABLE, "mytable1");
		conf1.set(TableInputFormat.SCAN_CACHEDROWS, "200");
		conf1.set(TableInputFormat.SCAN_COLUMNS, "mydatacolumn");

		conf2.set(TableInputFormat.INPUT_TABLE, "mytable2");
		conf2.set(TableInputFormat.SCAN_CACHEDROWS, "200");
		conf2.set(TableInputFormat.SCAN_COLUMNS, "moredatacolumn");

		MultiTableInputFormat.addTable("mytabl1", conf1, job);
		MultiTableInputFormat.addTable("mytabl2", conf2, job);

		boolean res = job.waitForCompletion(true);

		if (res == true) 
			return 0;
		else 
			return 1;
	}


	public class MultiTableMapper extends TableMapper<Text, Text> {

		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			/* Uses the family to find out what table is */
			if (value.containsColumn(toBytes("mydatacolumn"), toBytes("data"))) {
				context.write(new Text(Bytes.toString(key.get())), new Text(value.getValue(toBytes("mydatacolumn"), toBytes("data"))));
			} else if (value.containsColumn(toBytes("moredatacolumn"), toBytes("other_data"))) {
				context.write(new Text(Bytes.toString(key.get())), new Text(value.getValue(toBytes("moredatacolumn"), toBytes("other_data"))));
			}
		}
	}
	
	 public static class JoinedRecords extends Reducer<Text, Text, Text, Text> {
		 protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException ,InterruptedException {
			 List<String> data = new ArrayList<String>();

			 /*
			  * Output will be something like ROW_ID	value_from_mydtacolumn, value_from_moredatacolumn
			  */
			 context.write(key, new Text(StringUtils.join(values.iterator(), ",")));
		 }
	 }
}