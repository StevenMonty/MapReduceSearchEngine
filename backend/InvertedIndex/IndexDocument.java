import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IndexDocument implements Writable, WritableComparable<IndexDocument> {

    @Override
    public int compareTo(IndexDocument o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
