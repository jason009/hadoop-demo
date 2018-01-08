package com.jason.mr.dc;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <b><code>DataBean</code></b>
 * <p>
 * class_comment
 * <p>
 * <b>Creation Time:</b> 2018/1/3 15:13.
 *
 * @author yangjiangshui
 * @version ${Revision} 2018/1/3
 * @since spring-boot project_version
 */
public class DataBean implements Writable {
    private String telNo;
    private long upPayLoad;
    private long downPayLoad;
    private long totalPayLoad;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(telNo);
        dataOutput.writeLong(upPayLoad);
        dataOutput.writeLong(downPayLoad);
        dataOutput.writeLong(totalPayLoad);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.telNo = dataInput.readUTF();
        this.upPayLoad = dataInput.readLong();
        this.downPayLoad = dataInput.readLong();
        this.totalPayLoad = dataInput.readLong();

    }

    public String getTelNo() {
        return telNo;
    }

    public void setTelNo(String telNo) {
        this.telNo = telNo;
    }

    public long getUpPayLoad() {
        return upPayLoad;
    }

    public void setUpPayLoad(long upPayLoad) {
        this.upPayLoad = upPayLoad;
    }

    public long getDownPayLoad() {
        return downPayLoad;
    }

    public void setDownPayLoad(long downPayLoad) {
        this.downPayLoad = downPayLoad;
    }

    public long getTotalPayLoad() {
        return totalPayLoad;
    }

    public void setTotalPayLoad(long totalPayLoad) {
        this.totalPayLoad = totalPayLoad;
    }

    public DataBean() {
    }

    public DataBean(String telNo, long upPayLoad, long downPayLoad) {
        this.telNo = telNo;
        this.upPayLoad = upPayLoad;
        this.downPayLoad = downPayLoad;
        this.totalPayLoad = upPayLoad + downPayLoad;
    }

    @Override
    public String toString() {
        return upPayLoad + "\t" + downPayLoad +
                "\t" + totalPayLoad;
    }
}
