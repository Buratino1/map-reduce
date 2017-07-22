import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class ParquetReducer extends Reducer<Text, AvroValue<GenericRecord>, Void, Text> {
    private static final byte shift = 2 ;

    private TreeMap<Integer, AbstractMap.SimpleEntry<String, Integer>> rows = new TreeMap<Integer,AbstractMap.SimpleEntry<String, Integer>>();
    List<Integer> queue = new LinkedList<Integer>();
    private String adj = "";
    private int lastValue = -1;


    @Override
    protected void reduce(Text key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
        for (AvroValue<GenericRecord> value : values) {
            rows.put((Integer) value.datum().get("id"),
                      new AbstractMap.SimpleEntry(value.datum().get("type"), value.datum().get("value"))) ;
        }

        for(Map.Entry<Integer, AbstractMap.SimpleEntry<String, Integer>> entry : rows.entrySet()) {
            AbstractMap.SimpleEntry<String, Integer> rowValue = entry.getValue();

            if (rowValue.getKey().equals("original")) {
                lastValue = rowValue.getValue() ;
                queue.add(lastValue) ;
                adj = "" ;
            } else {
                adj = " " + String.valueOf(lastValue);
                if (queue.size()- shift >0) {
                    adj = adj + " " + queue.get(queue.size()-shift).toString() ;
                }
            }
            Text output = new Text(entry.getKey()+" "+rowValue.getKey() + " " + rowValue.getValue() + adj);

            context.write(null, output );
        }
    }
}
