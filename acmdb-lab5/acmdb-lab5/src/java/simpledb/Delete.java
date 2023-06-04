package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private TupleDesc tupleDesc;

    private int count;
    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"number of deleted records"});
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(called){
            return null;
        }
        called = true;
        while(this.child.hasNext()){
            Tuple tuple1 = this.child.next();
            try{
                Database.getBufferPool().deleteTuple(tid, tuple1);
                count ++;
            }
            catch (IOException e){
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
