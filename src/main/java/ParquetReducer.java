import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class ParquetReducer extends Reducer<Text, AvroValue<GenericRecord>, Void, Text> {
    private TreeMap<Integer, AbstractMap.SimpleEntry<String, Integer>> rows = new TreeMap<Integer,AbstractMap.SimpleEntry<String, Integer>>();
    private String adj = "";
    private Integer lastValue = -1;

    @Override
    protected void reduce(Text key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
        for (AvroValue<GenericRecord> value : values) {
            AbstractMap.SimpleEntry<String, Integer> pair = new AbstractMap.SimpleEntry(value.datum().get("type"), value.datum().get("value")) ;
            rows.put((Integer) value.datum().get("id") , pair) ;
        }

        for(Map.Entry<Integer, AbstractMap.SimpleEntry<String, Integer>> entry : rows.entrySet()) {
            AbstractMap.SimpleEntry<String, Integer> rowValue = entry.getValue();

            if (rowValue.getKey() == "original") {
                lastValue = rowValue.getValue() ;
                adj = "" ;
            } else {
                adj = " " + lastValue.toString();
            }
            Text output = new Text(entry.getKey()+" "+rowValue.getKey() + " " + rowValue.getValue() + adj);

            context.write(null, output );
        }
    }
}
