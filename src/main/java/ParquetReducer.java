import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
public class ParquetReducer extends Reducer<Text, AvroValue<GenericRecord>, Void, GenericRecord> {

    /**
     * Does nothing but pass the values through.  Do some more processing here.
     *
     * @param key     the reducer's key
     * @param values  all of this key's records as an Avro representation of the Parquet record
     * @param context the reducer's context
     */
    @Override
    protected void reduce(Text key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
        for (AvroValue<GenericRecord> value : values) {
            GenericRecord record = value.datum();

            int x = (Integer) record.get("x") ;

            if ( x < 1000) {
                context.write(null, value.datum());
            }

            if ( record.get("y").equals("09/09/2014") ) {
                context.write(null, value.datum());
            }
        }
    }
}
