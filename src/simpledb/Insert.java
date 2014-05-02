package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    
    private TransactionId _trans_id;
    private DbIterator _oppairator;
    private int _oppai_id;
    private Tuple tup;
    private boolean _groped;
    
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	_trans_id = t;
    	_oppairator = child;
    	_oppai_id = tableid;
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
    	_groped = false;
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
     * Inserts tuples read from child into the tableid specified by the
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
    	if (_groped) return null;
    	int inserts = 0;
    	BufferPool bf = Database.getBufferPool();
    	while (_oppairator.hasNext()){
    		try {
    			bf.insertTuple(_trans_id, _oppai_id, _oppairator.next());
    			inserts++;
    		}
    		catch (IOException e){
    			e.printStackTrace();
    		}
    	}
    	tup.setField(0, new IntField(inserts));
    	_groped = true;
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
