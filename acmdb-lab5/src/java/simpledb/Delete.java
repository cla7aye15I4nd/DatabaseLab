package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private DbIterator child;    

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
        this.t = t;
        this.child = child;        
        this.called = false;    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[] { Type.INT_TYPE });
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();    
    }

    public void close() {
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
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
        if (called) return null;

        called = true;
        Tuple tuple = new Tuple (getTupleDesc());

        int count = 0;
        while (child.hasNext()) {            
            try { Database.getBufferPool().deleteTuple(t, child.next()); }
            catch (IOException e) {}            
            count++;
        }
        
        tuple.setField(0, new IntField(count));
        return tuple;
    }

    @Override
    public DbIterator[] getChildren() {        
        return new DbIterator[] { child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    }

}
