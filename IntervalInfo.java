public class IntervalInfo
{
    int start;
    int end;
    int node_id;
    int index;

    public IntervalInfo(int node_id, int index)
    {
        this.node_id = node_id;
        this.index = index;
        this.start = (node_id + (int)Math.pow(2, index)) % 256;
        this.end = (node_id + (int)Math.pow(2, index+1)) % 256;
    }

    /* 
     * Returns whether or not a given id falls
     * within this interval [start,end).
     */
    public boolean contains(int id)
    {
        return   (this.start < this.end && id >= this.start && id < this.end)
               ||(this.start > this.end && (id >= this.start || id < this.end));
    }
}
