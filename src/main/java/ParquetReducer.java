import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ParquetReducer extends Reducer<Text, AvroValue<GenericRecord>, Void, GenericRecord> {

    @Override
    protected void reduce(Text key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {

        Queue<Integer> queue = new LinkedList<Integer>();
        List<Integer> rows = new ArrayList<Integer>();
        AvroValue<GenericRecord> rn ;

        Integer prevValue = 0;

        for (AvroValue<GenericRecord> value : values) {
            Integer nId  = (Integer) value.datum().get("id") ;
            Integer nVal = (Integer) value.datum().get("value") ;
            String sType = (String) value.datum().get("type") ;
            rows.add(nId, nVal) ;
            rn = value ;
        }

        for (AvroValue<GenericRecord> value : values) {
            context.write(null, value.datum());
        }
    }
}
