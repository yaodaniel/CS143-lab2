package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */

	private int num_pages;
	private TupleDesc td;
	private File f;
	//private HeapPage page;
	//private ArrayList<Page> pages;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f = f;
    	this.td = td;
    	//pages = new ArrayList<Page>();
    	this.num_pages = (int) (f.length()/BufferPool.getPageSize() + .5);
    	//cur_page = 0;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	int page_size = BufferPool.getPageSize();
    	int num_pages_on_file = (int) (f.length()/BufferPool.getPageSize() + .5);
    	byte [] data = HeapPage.createEmptyPageData();
    	try {
    		if (pid.pageNumber() < num_pages_on_file) {
    			RandomAccessFile raf = new RandomAccessFile(f, "r");
				raf.skipBytes(page_size*pid.pageNumber());
				raf.read(data);
				raf.close();
    		} 
			return new HeapPage((HeapPageId) pid, data);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	//Current writes the updated file back, but does not remove it from HeapFile
    	RandomAccessFile raf = new RandomAccessFile(f, "rw");
    	int page_size = BufferPool.getPageSize(); //Size (in bytes) per page as deemed by BufferPool
    	byte [] page_data = page.getPageData(); //Gets the data in bytes of the page we're trying to write
    	int offset = page_size*page.getId().pageNumber(); //Returns the offset from where we should start writing on f
    	try {
    		raf.skipBytes(offset);
    		raf.write(page_data);
    		raf.close();
    	} catch (FileNotFoundException e) {
    		e.printStackTrace();
    	} /*catch (IOException e) {
    		e.printStackTrace();
    	}*/
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        //return (int) (f.length()/BufferPool.getPageSize() + .5);
    	return num_pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	 // not necessary for lab1
    	ArrayList<Page> al = new ArrayList<Page>();
    	for (int i = 0; i < numPages(); i++){
    		PageId pg_id = new HeapPageId(getId(), i);
        	HeapPage pg = (HeapPage)Database.getBufferPool().getPage(tid, pg_id, Permissions.READ_WRITE);
        	try { 
        		pg.insertTuple(t);
        		al.add(pg);
        		return al;
        	}
        	catch (DbException e){
        		continue;
        	}
    	}
    	PageId pg_id = new HeapPageId(getId(), num_pages);
    	HeapPage pg = (HeapPage)Database.getBufferPool().getPage(tid, pg_id, Permissions.READ_WRITE);
    	pg.insertTuple(t);
		al.add(pg);
		num_pages++;
        return al;
       
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	ArrayList<Page> al = new ArrayList<Page>();
    	PageId pg_id = t.getRecordId().getPageId();
    	HeapPage pg = (HeapPage)Database.getBufferPool().getPage(tid, pg_id, Permissions.READ_WRITE);
    	pg.deleteTuple(t);
        // some code goes here
        // not necessary for lab1
    	return al;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	DbFileIterator ss = new HeapFileIter(tid, getId());
        return ss;
    }

}