package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class AggregatorIter implements DbIterator{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Iterator<Field> it;
    Iterable<Field> fields = null;
	public AggregatorIter(Iterable<Field> f){
		this.fields = f;
	}
	@Override
	public void open() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
        it = fields.iterator();
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		return it.hasNext();
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException,
			NoSuchElementException {
		// TODO Auto-generated method stub
		return it.next();
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TupleDesc getTupleDesc() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
}
