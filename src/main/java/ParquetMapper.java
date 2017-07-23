import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ParquetMapper extends Mapper<LongWritable, GenericRecord, Text, AvroValue<GenericRecord>> {
    private final Text outputKey = new Text();
    private final AvroValue<GenericRecord> outputValue = new AvroValue<GenericRecord>();

    @Override
    protected void map(LongWritable key, GenericRecord value, Context context) throws IOException, InterruptedException {
        outputKey.set(String.valueOf(value.get("position")));
        outputValue.datum(value);
        context.write(outputKey, outputValue);
    }
}
