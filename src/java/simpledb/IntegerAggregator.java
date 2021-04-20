package simpledb;

import java.util.HashMap;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    
    private HashMap<Field, Info> group;    

    private class Info {
        public int value;
        private int count;        
        private int sum;

        public Info () {
            switch (what) {
                case MIN: value = Integer.MAX_VALUE; break;
                case MAX: value = Integer.MIN_VALUE; break;
                default: value = 0;
            }            
            count = 0;
            sum = 0;
        }

        public void add (int newValue) {
            sum += newValue;
            count++;

            switch (what) {
                case MIN:  value = Math.min(value, newValue); break;
                case MAX:  value = Math.max(value, newValue); break;
                case SUM:  value += newValue;                 break;
                case AVG:  value = sum / count;               break;
                case COUNT:value ++;
            }
        }
    };

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.group = new HashMap<>();        
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field field = gbfieldtype == null ? null: tup.getField(gbfield);
        
        if (!group.containsKey(field))
            group.put(field, new Info());

        group.get(field).add(((IntField) tup.getField(afield)).getValue());
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        TupleDesc td = null;
        ArrayList<Tuple> tuples = new ArrayList<>();

        if (gbfieldtype == null) {
            td = new TupleDesc (new Type[] {Type.INT_TYPE});
            Tuple tuple = new Tuple (td);

            if (group.containsKey(null)) 
                tuple.setField(0, new IntField(group.get(null).value));
            tuples.add(tuple);
        } else {
            td = new TupleDesc (new Type[] {gbfieldtype, Type.INT_TYPE});
            for (HashMap.Entry<Field, Info> e : group.entrySet()) {
                Tuple tuple = new Tuple (td);
                
                tuple.setField(0, e.getKey());
                tuple.setField(1, new IntField(e.getValue().value));
                tuples.add(tuple);
            }
        }

        return new TupleIterator (td, tuples);
    }

}
