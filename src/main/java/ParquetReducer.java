import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ParquetReducer extends Reducer<Text, AvroValue<GenericRecord>, Void, GenericRecord> {

    @Override
    protected void reduce(Text key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {

        Queue<String> queue = new LinkedList<String>();
        Integer prevValue = 0;

        for (AvroValue<GenericRecord> value : values) {
            if (value.datum().get("type") == "original") {
                queue.add(value.datum().get("value").toString()) ;

            } else {
                prevValue = Integer.parseInt(queue.remove());
            }
            context.write(null, value.datum());
        }
    }
}
