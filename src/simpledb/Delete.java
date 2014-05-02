package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private TransactionId _trans_id;
    private DbIterator _oppairator;
    private Tuple tup;
    private boolean _squeezed;
    
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	_trans_id = t;
    	_oppairator = child;
    	Type [] type = {Type.INT_TYPE};
    	String [] s = {null};
    	TupleDesc td = new TupleDesc(type, s);
    	tup = new Tuple(td);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tup.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	_oppairator.open();
    	_squeezed = false;
    }

    public void close() {
        // some code goes here
    	super.close();
    	_oppairator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	_oppairator.rewind();
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
    	if (_squeezed) return null;
    	int inserts = 0;
    	BufferPool bf = Database.getBufferPool();
    	while (_oppairator.hasNext()){
    		try {
    			bf.deleteTuple(_trans_id, _oppairator.next());
    			inserts++;
    		}
    		catch (IOException e){
    			e.printStackTrace();
    		}
    	}
    	tup.setField(0, new IntField(inserts));
    	_squeezed = true;
        return tup;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator [] dbI = {_oppairator};
        return dbI;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	_oppairator = children[0];
    }

}
