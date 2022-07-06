import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JobKey implements WritableComparable<JobKey> {
    public Text jobId = new Text();
    public IntWritable recordType = new IntWritable();

    public JobKey(){}

    public JobKey(String jobId, IntWritable recordType){
        this.jobId.set(jobId);
        this.recordType = recordType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.jobId.write(out);
        this.recordType.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.jobId.readFields(in);
        this.recordType.readFields(in);
    }

    @Override
    public int compareTo(JobKey other) {
        if (this.jobId.equals(other.jobId)){
            return this.recordType.compareTo(other.recordType);
        }
        else {
            return this.jobId.compareTo(other.jobId);
        }
    }

    public boolean equals(JobKey other){
        return this.jobId.equals(other.jobId) &&
                this.recordType.equals(other.recordType);
    }
    public int hashCode(){
        return this.jobId.hashCode();
    }

    public static final IntWritable PEOPLE_RECORD = new IntWritable(1);
    public static final IntWritable SALARY_RECORD = new IntWritable(0);

}