import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;

import java.util.List;

 // yarn jar target/testmr-1.0-SNAPSHOT.jar ParquetJob -libjars
 // /usr/hdp/2.6.0.3-8/hadoop/lib/parquet-avro-1.8.1.jar,/usr/hdp/2.6.0.3-8/hadoop/lib/parquet-hadoop-1.8.1.jar,/usr/hdp/2.6.0.3-8/hadoop/lib/parquet-common-1.8.1.jar,/usr/hdp/2.6.0.3-8/hadoop/lib/parquet-column-1.8.1.jar,/usr/hdp/2.6.0.3-8/hadoop/lib/avro-mapred-1.8.1.jar
 // /ganer/mvdb /ganer/out2

public class ParquetJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ParquetJob(), args);
        System.exit(res);
    }

    /**
     * @param args the arguments, which consist of the input file or directory and the output directory
     * @return the job return status, 0 for success, 1 for failure
     * @throws Exception
     */
    // @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(getConf());
        job.setJarByClass(ParquetJob.class);


        // Parquet Schema
        // we assume a single schema for all files
        List<Footer> footers = ParquetFileReader.readFooters(getConf(), new Path(args[0]));
        MessageType schema = footers.get(0).getParquetMetadata().getFileMetaData().getSchema();

        // Avro Schema
        // convert the Parquet schema to an Avro schema
        AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();
        Schema avroSchema = avroSchemaConverter.convert(schema);

        // Mapper
        job.setMapperClass(ParquetMapper.class);
        // Input
        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path(args[0]));
        AvroParquetInputFormat.setAvroReadSchema(job, avroSchema);

        // Intermediate Output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputValueSchema(job, avroSchema);

        // Reducer
        job.setReducerClass(ParquetReducer.class);
        // Output
        // job.setOutputFormatClass(AvroParquetOutputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // AvroParquetOutputFormat.setSchema(job, avroSchema);
        // AvroParquetOutputFormat.setOutputPath(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Text.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
