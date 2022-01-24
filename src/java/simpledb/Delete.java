package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TupleDesc td;
    private TransactionId trid;
    private OpIterator child;
    private boolean called = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.trid = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        super.close();
        child.rewind();
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
        // Similarly to Insert, all tuples are actually deleted here
        if (called)
            return null;
        called = true;
        int count = 0;
        BufferPool bp = Database.getBufferPool();
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
                bp.deleteTuple(trid,t);
                count++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple count_tuple = new Tuple(td);
        count_tuple.setField(0,new IntField(count));
        return count_tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child=children[0];
    }

}
