package simpledb;

import java.io.Serializable;
import java.util.Iterator;

// import org.omg.CORBA.INTERNAL;

import java.util.*;

/**
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 */
public interface Aggregator extends Serializable {
    static final int NO_GROUPING = -1;

    /**
     * SUM_COUNT and SC_AVG will
     * only be used in lab7, you are not required
     * to implement them until then.
     * */
    public enum Op implements Serializable {
        MIN, MAX, SUM, AVG, COUNT,
        /**
         * SUM_COUNT: compute sum and count simultaneously, will be
         * needed to compute distributed avg in lab7.
         * */
        SUM_COUNT,
        /**
         * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
         * will be used to compute distributed avg in lab7.
         * */
        SC_AVG;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }
        
        public String toString()
        {
        	if (this==MIN)
        		return "min";
        	if (this==MAX)
        		return "max";
        	if (this==SUM)
        		return "sum";
        	if (this==SUM_COUNT)
    			return "sum_count";
        	if (this==AVG)
        		return "avg";
        	if (this==COUNT)
        		return "count";
        	if (this==SC_AVG)
    			return "sc_avg";
        	throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup);

    /**
     * Create a DbIterator over group aggregate results.
     * @see simpledb.TupleIterator for a possible helper
     */
    public DbIterator iterator();
    
}

class AggregateIterator implements DbIterator{
    Iterator<HashMap.Entry<Field, Integer>> iterator;
    TupleDesc tupleDesc;
    private HashMap<Field, Integer> groupHashMap;
    private Type iteratorgbFieldType;

    public AggregateIterator(HashMap<Field, Integer> groupHashMap, Type gbFieldType){
        this.groupHashMap = groupHashMap;
        this.iteratorgbFieldType = gbFieldType;

        if (this.iteratorgbFieldType == null){
            this.tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"aggregateVal"});
        }
        else{
            this.tupleDesc = new TupleDesc(new Type[] {this.iteratorgbFieldType, Type.INT_TYPE}, new String[] {"groupVal", "aggregateVal"});
        }
    }

    public void open() throws DbException, TransactionAbortedException{
        this.iterator = groupHashMap.entrySet().iterator();
    }

    public void close(){
        this.iterator = null;
        this.tupleDesc = null;
    }

    public void rewind()throws DbException, TransactionAbortedException{
        this.iterator = groupHashMap.entrySet().iterator();
    }

    public TupleDesc getTupleDesc(){
        return this.tupleDesc;
    }

    public boolean hasNext() throws DbException, TransactionAbortedException{
        return iterator.hasNext();
    }

    public Tuple next() throws DbException, TransactionAbortedException{
        HashMap.Entry<Field, Integer> entry = iterator.next();
        Field field = entry.getKey();
        Tuple tuple = new Tuple(tupleDesc);
        this.setFields(tuple, entry.getValue(), field);
        return tuple;
    }

    public void setFields(Tuple t, int value, Field f){
        if (f == null){
            t.setField(0, new IntField(value));
        }
        else{
            t.setField(0, f);
            t.setField(1, new IntField(value));
        }
    }
}