package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    
    private Predicate _pred;
    private DbIterator _child;
    
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
    	_pred = p;
    	_child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return _pred;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return _child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	_child.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	_child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	_child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	while(_child.hasNext()){
    		Tuple t = _child.next();
    		if (_pred.filter(t)) return t;
    	}
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator[] dbI = {_child};
        return dbI;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	_child = children[0];
    }

}
