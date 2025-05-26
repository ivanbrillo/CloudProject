package it.unipi.hadoop;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@NoArgsConstructor
@Setter
@Getter
public class FileCountData implements Writable, Comparable<FileCountData> {
    private String filename;
    private long count;

    // Constructor to initialize the filename and count
    public FileCountData(String filename, long count) {
        this.filename = filename;
        this.count = count;
    }
    
    // Deserialize the object from Hadoop's binary format
    @Override
    public void readFields(DataInput in) throws IOException {
        filename = in.readUTF();    // read the filename
        count = in.readLong();      // read the count
    }

    // Serialize the object to Hadoop's binary format
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(filename); // write the filename as UTF-8 string
        out.writeLong(count);   // write the count   
    }

    // Method to compare two FileCountData objects for sorting purposes
    @Override
    public int compareTo(FileCountData o) {
        return this.filename.compareTo(o.filename);
    }
}