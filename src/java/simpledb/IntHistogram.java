package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int min;
    private int max;
    private int ntups;
    private int length;
    private int[] fenwick;
    private double width;    

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        if (max - min + 1 <= buckets) 
            this.width = (max - min + 1) / buckets;
        else {
            buckets = max - min + 1;
            this.width = 1;
        }

        this.min = min;
        this.max = max;
        this.ntups = 0;
        this.length = buckets;
        this.fenwick = new int [buckets * 2];
    }

    private int lowbit(int v) {
        return v & -v;
    }

    private double getPrefixSum(int v) {
        double sum = 0.0;
        while (v != 0) {
            sum += fenwick[v];
            v -= lowbit(v);
        }
        return sum;
    }
    
    private int getIndex(int v) {
        int id = (int) ((v - min) / width) + 1;

        if (id < 1) return 1;
        if (id > length) return length;

        return id;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	ntups++;
        v = getIndex(v);
        while (v <= length) {
            fenwick[v]++;
            v += lowbit(v);
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (op == Predicate.Op.LESS_THAN_OR_EQ)
            return 	estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);                
        if (op == Predicate.Op.GREATER_THAN_OR_EQ)
            return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
        if (op == Predicate.Op.GREATER_THAN)
            return 1.0 - estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
        if (op == Predicate.Op.NOT_EQUALS)
            return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
        
        int i = getIndex(v);
        if (op == Predicate.Op.EQUALS) {
            if (v < min || v > max) return 0;
            return (getPrefixSum(i) - getPrefixSum(i-1)) / width / ntups;
        }

        if (op == Predicate.Op.LESS_THAN) {
            if (v < min) return 0.0;
            if (v > max) return 1.0;
            double a = getPrefixSum(i);
            double b = getPrefixSum(i-1);

            return (b + (a - b) * (v - (min + width * (i - 1)))) / width / ntups;
        }

        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
