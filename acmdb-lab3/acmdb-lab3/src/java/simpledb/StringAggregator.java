package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private HashMap<Field, Integer> groupHashMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT){
            throw new IllegalArgumentException("What != COUNT !");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.groupHashMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field mergeField = null;
        if (gbfield != NO_GROUPING){
            mergeField = tup.getField(gbfield);
        }
        if (mergeField != null && mergeField.getType() != gbfieldtype){
            throw new IllegalArgumentException("Wrong fieldtype.");
        }
        if (!this.groupHashMap.containsKey(mergeField)){
            this.groupHashMap.put(mergeField, 1);
        }
        else{
            this.groupHashMap.put(mergeField,this.groupHashMap.get(mergeField) + 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new AggregateIterator(groupHashMap, gbfieldtype);
    }
}


