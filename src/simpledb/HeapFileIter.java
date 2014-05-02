package simpledb;

import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;

public class HeapFileIter implements DbFileIterator {

	
	private int _tableid, _pgNo;
	private TransactionId _transid;
	private ListIterator<Tuple> _tuples;
	
	public HeapFileIter(TransactionId transid, int tableid){
		_tableid = tableid;
		_transid = transid;
		_pgNo = 0;
	}
	
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		PageId pgId = new HeapPageId(_tableid, _pgNo);
    	HeapPage pg = (HeapPage)Database.getBufferPool().getPage(_transid, pgId, Permissions.READ_ONLY);
    	_pgNo++;
    	_tuples = (ListIterator<Tuple>) pg.iterator();
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if (_tuples == null) return false;
		
		HeapFile file = (HeapFile)Database.getCatalog().getDatabaseFile(_tableid);
		while(!_tuples.hasNext() && _pgNo < file.numPages()){
			open();
		}
		if (_tuples.hasNext()) return true;
		return false;
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException,
			NoSuchElementException {
		if (_tuples == null)
			throw new NoSuchElementException();
		return _tuples.next();
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		_pgNo = 0;
		open();
	}

	@Override
	public void close() {
		_tuples = null;
		this._pgNo = 0;
	}
}
