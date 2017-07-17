import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ParquetReducer extends Reducer<Text, AvroValue<GenericRecord>, Void, Text> {
    private TreeMap<Integer, Pair<String, Integer>> rows = new TreeMap<Integer, Pair<String, Integer>>();

    @Override
    protected void reduce(Text key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
        // Map<Integer, Integer> rows = new TreeMap<Integer, Integer>();

        GenericRecord record = null;
        for (AvroValue<GenericRecord> value : values) {
            // Integer nId  = (Integer) value.datum().get("id") ;
            rows.put((Integer) value.datum().get("id") , new Pair<String, Integer>(value.datum().get("type"), value.datum().get("value") ) ) ;
        }

        for(Map.Entry<Integer, Pair<String, Integer>> entry : rows.entrySet()) {
            Integer rowId = entry.getKey();
            Pair<String, Integer> rowValue = entry.getValue();
            Text output = new Text(rowId+" "+rowValue.toString());
            context.write(null, output );
        }
    }
}
