package flowSum;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhangguanlong on 2017/11/21.
 */
public class FlowBean implements WritableComparable<FlowBean>{

    private String number;
    private long up_flow;
    private long d_flow;
    private long s_flow;
    //反射机制需要调用空参构造
    public FlowBean(){

    }
    //带参构造
    public FlowBean(String number, long up_flow, long d_flow) {
        this.number = number;
        this.up_flow = up_flow;
        this.d_flow = d_flow;
        this.s_flow = d_flow+up_flow;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public long getUp_flow() {
        return up_flow;
    }

    public void setUp_flow(long up_flow) {
        this.up_flow = up_flow;
    }

    public long getD_flow() {
        return d_flow;
    }

    public void setD_flow(long d_flow) {
        this.d_flow = d_flow;
    }

    public long getS_flow() {
        return s_flow;
    }

    public void setS_flow(long s_flow) {
        this.s_flow = s_flow;
    }
    //将对象数据序列化到流中
    public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(number);
    dataOutput.writeLong(up_flow);
    dataOutput.writeLong(d_flow);
    dataOutput.writeLong(s_flow);
    }
    //从数据流中反序列化出对象的数据
    //读的顺序与序列化时保持一致
    public void readFields(DataInput dataInput) throws IOException {
    number=dataInput.readUTF();
    up_flow=dataInput.readLong();
    d_flow=dataInput.readLong();
    s_flow=dataInput.readLong();

    }

    @Override
    public String toString() {
        return "    "+up_flow+"     "+d_flow+"  "+s_flow;
    }

    /**
     * Compares this object with the specified object for order.  Returns a
     * negative integer, zero, or a positive integer as this object is less
     * than, equal to, or greater than the specified object.
     * <p>
     * <p>The implementor must ensure <tt>sgn(x.compareTo(y)) ==
     * -sgn(y.compareTo(x))</tt> for all <tt>x</tt> and <tt>y</tt>.  (This
     * implies that <tt>x.compareTo(y)</tt> must throw an exception iff
     * <tt>y.compareTo(x)</tt> throws an exception.)
     * <p>
     * <p>The implementor must also ensure that the relation is transitive:
     * <tt>(x.compareTo(y)&gt;0 &amp;&amp; y.compareTo(z)&gt;0)</tt> implies
     * <tt>x.compareTo(z)&gt;0</tt>.
     * <p>
     * <p>Finally, the implementor must ensure that <tt>x.compareTo(y)==0</tt>
     * implies that <tt>sgn(x.compareTo(z)) == sgn(y.compareTo(z))</tt>, for
     * all <tt>z</tt>.
     * <p>
     * <p>It is strongly recommended, but <i>not</i> strictly required that
     * <tt>(x.compareTo(y)==0) == (x.equals(y))</tt>.  Generally speaking, any
     * class that implements the <tt>Comparable</tt> interface and violates
     * this condition should clearly indicate this fact.  The recommended
     * language is "Note: this class has a natural ordering that is
     * inconsistent with equals."
     * <p>
     * <p>In the foregoing description, the notation
     * <tt>sgn(</tt><i>expression</i><tt>)</tt> designates the mathematical
     * <i>signum</i> function, which is defined to return one of <tt>-1</tt>,
     * <tt>0</tt>, or <tt>1</tt> according to whether the value of
     * <i>expression</i> is negative, zero or positive.
     *
     * @param o the object to be compared.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     * @throws NullPointerException if the specified object is null
     * @throws ClassCastException   if the specified object's type prevents it
     *                              from being compared to this object.
     */
    public int compareTo(FlowBean o) {

        return s_flow>o.s_flow?-1:1;
    }
}
