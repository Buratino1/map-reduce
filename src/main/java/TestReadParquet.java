import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.Log;
import parquet.example.data.Group;
import parquet.hadoop.example.ExampleInputFormat;

import java.io.IOException;

public class TestReadParquet extends Configured
        implements Tool {
    private static final Log LOG =
            Log.getLog(TestReadParquet.class);

    /*
     * DGO test1
     * Read a Parquet record
     */
    public static class MyMap extends
            Mapper<LongWritable, Group, LongWritable, Text> {

        @Override
        public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
            NullWritable outKey = NullWritable.get();
            String outputRecord = "";
            // Get the schema and field values of the record
            String inputRecord = value.toString();
            // Process the value, create an output record
            // ...
            context.write(key, new Text(outputRecord));

        /*
            LongWritable outKey = key;
            String field1 = value.getString("caseId", 0);
            context.write(outKey, new Text(field1+";"));
        */
        }
    }

    public static class MyRed extends
            Reducer<LongWritable, Group, LongWritable, Text> {
        public void reduce(LongWritable key, Group value, Mapper.Context context)
                throws IOException, InterruptedException {

            String caseId = value.getString("caseId", 0);
            if (caseId.equals("6624287032")) {

                context.write(key, new Text("test"));

            }
        }
    }

    public int run(String[] args) throws Exception {

        Job job = new Job(getConf());

        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyRed.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(ExampleInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), new TestReadParquet(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }
}
