package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.*;
import java.util.List;

// import javax.swing.text.html.HTMLDocument.Iterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    
    private HashMap<Field, Integer> groupHashMap;
    private HashMap<Field, List<Integer>> averageHashMap; 
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or27
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
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupHashMap = new HashMap<>();
        this.averageHashMap = new HashMap<>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField tupafield = (IntField)tup.getField(afield);
        Field tupgbField = null;
        if (gbfield != NO_GROUPING){
            tupgbField = tup.getField(this.gbfield);
        }
        int mergeValue = tupafield.getValue();
        
        if (what == Op.MAX){
            if (!groupHashMap.containsKey(tupgbField)){
                groupHashMap.put(tupgbField, mergeValue);
            }
            else{
                groupHashMap.put(tupgbField, Math.max(groupHashMap.get(tupgbField), mergeValue));
            }
        }
        else if (what == Op.MIN){
            if (!groupHashMap.containsKey(tupgbField)){
                groupHashMap.put(tupgbField, mergeValue);
            }
            else{
                groupHashMap.put(tupgbField, Math.min(groupHashMap.get(tupgbField), mergeValue));
            }
        }
        else if (what == Op.SUM){
            if (!groupHashMap.containsKey(tupgbField)){
                groupHashMap.put(tupgbField, mergeValue);
            }
            else{
                groupHashMap.put(tupgbField, (groupHashMap.get(tupgbField) + mergeValue));
            }
        }
        else if (what == Op.COUNT){
            if (!groupHashMap.containsKey(tupgbField)){
                groupHashMap.put(tupgbField, 1);
            }
            else{
                groupHashMap.put(tupgbField, groupHashMap.get(tupgbField) + 1);
            }
        }
        else if (what == Op.AVG){
            if (!averageHashMap.containsKey(tupgbField)){
                List<Integer> list = new ArrayList<>();
                list.add(mergeValue);
                averageHashMap.put(tupgbField, list);
            }
            else{
                List<Integer> list = averageHashMap.get(tupgbField);
                list.add(mergeValue);
            }
        }
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
        // some code goes here
        return new IntegerAggregatorIterator();
    }

    private class IntegerAggregatorIterator extends AggregateIterator{
        private Iterator<HashMap.Entry<Field,List<Integer>>> averageIterator;
        private boolean isAverage;
        
        public IntegerAggregatorIterator(){
            super(groupHashMap, gbfieldtype);
            if (what == Op.AVG){
                this.isAverage = true;
            }
            else{
                this.isAverage = false;
            }
        }

        public void open() throws DbException, TransactionAbortedException{
            super.open();
            if (isAverage){
                averageIterator = averageHashMap.entrySet().iterator();
            }
            else{
                averageIterator = null;
            }
        }

        public boolean hasNext() throws DbException, TransactionAbortedException{
            if (isAverage){
                return averageIterator.hasNext();
            }
            return super.hasNext();
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
            Tuple nextTuple = new Tuple(this.tupleDesc);
            if (isAverage){
                HashMap.Entry<Field, List<Integer>> averageEntry = this.averageIterator.next();
                Field averageField = averageEntry.getKey();
                List<Integer> averageList = averageEntry.getValue();

                int value = sumList(averageList) / averageList.size();
                setFields(nextTuple, value, averageField);
                return nextTuple;
            }
            else{
                return super.next();
            }
        }

        public void close(){
            super.close();
            averageIterator = null;
        }

        public void rewind() throws DbException, TransactionAbortedException{
            super.rewind();
            if (isAverage){
                averageIterator = averageHashMap.entrySet().iterator();
            }
        }

        private int sumList(List<Integer> List){
            int sum = 0;
            for (int i :List){
                sum += i;
            }
            return sum;
        }
    }

}
