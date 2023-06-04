package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private int tableId;
    private TupleDesc tupleDesc;

    private int count;
    private boolean called;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))){
            throw new DbException("TupleDesc of child differs from table into which we are to insert.");
        }
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"number of inserted records"});
        this.count = -1;
        this.called = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.count = 0;
        this.child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.count = -1;
        this.called = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.count = 0;
        this.called = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (called){
            return null;
        }
        called = true;
        while(child.hasNext()){
            Tuple tuple1 = child.next();
            try{
                Database.getBufferPool().insertTuple(tid, tableId, tuple1);
                count ++;
            }
            catch(IOException e){
                e.printStackTrace();
                break;
            }
        }
        Tuple tuple2 = new Tuple(tupleDesc);
        tuple2.setField(0, new IntField(count));
        return tuple2;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
