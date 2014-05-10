package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public static class Prio implements Comparable{
    	protected int prio;
    	public PageId pid;
    	public Prio(int prio, PageId pid){
    		this.prio = prio;
    		this.pid = pid;
    	}
    	public int compareTo(Object arg0){
    		Prio other = (Prio) arg0;
    		if(other.prio == prio) return 0;
    		else if(other.prio > prio) return -1;
    		else return 1;
    	}
    }
    
    private int prio;
    private HashMap<Integer, Page> pages;
    private HashMap<TransactionId, ArrayList<PageId>> tits;
    private PriorityQueue<Prio> prior;
    
    
    public BufferPool(int numPages) {
        // some code goes here
    	prio = 0;
    	pages = new HashMap<Integer, Page>();
    	tits = new HashMap<TransactionId, ArrayList<PageId>>();
    	prior = new PriorityQueue<Prio>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	if (!pages.containsKey(pid.hashCode())){
    		if (pages.size() == DEFAULT_PAGES)
    			//throw new DbException("Page limit reached");
    			evictPage();
    		DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
    		pages.put(pid.hashCode(), dbFile.readPage(pid));
    		if (!tits.containsKey(tid)){
    			tits.put(tid, new ArrayList<PageId>());
    		}
    		tits.get(tid).add(pid);
    	}
    	prior.add(new Prio(prio, pid));
    	prio++;
        return pages.get(pid.hashCode());
    }
    /*
    / /\
    / / /
   / / /   _
  /_/ /   / /\
  \ \ \  / /  \
   \ \ \/ / /\ \
_   \ \ \/ /\ \ \
/_/\   \_\  /  \ \ \
\ \ \  / /  \   \_\/
\ \ \/ / /\ \
\ \ \/ /\ \ \
 \ \  /  \ \ \
  \_\/   / / /
        / / /
       /_/ /
       \_\/*/

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile db_file = Database.getCatalog().getDatabaseFile(tableId);
    	db_file.insertTuple(tid, t);
    	pages.get(t.getRecordId().getPageId().hashCode()).markDirty(true, tid);
    	if (!tits.containsKey(tid)){
    		tits.put(tid, new ArrayList<PageId>());
    	}
    	tits.get(tid).add(t.getRecordId().getPageId());
    	prior.add(new Prio(prio, t.getRecordId().getPageId()));
    	prio++;
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	int table_id = t.getRecordId().getPageId().getTableId();
    	DbFile db_file = Database.getCatalog().getDatabaseFile(table_id);
    	db_file.deleteTuple(tid, t);
    	pages.get(t.getRecordId().getPageId().hashCode()).markDirty(true, tid);
    	if (!tits.containsKey(tid)){
    		tits.put(tid, new ArrayList<PageId>());
    	}
    	tits.get(tid).add(t.getRecordId().getPageId());
    	prior.add(new Prio(prio, t.getRecordId().getPageId()));
    	prio++;
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    //TODO
        // some code goes here
        // not necessary for lab1
    	for(Page p : pages.values()){
    		if(p.isDirty() != null)
    			flushPage(p.getId());
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
    //TODO
        // some code goes here
        // not necessary for lab1
    	Page pageToBeFlushed = pages.get(pid.hashCode());
    	DbFile tmp = Database.getCatalog().getDatabaseFile(pid.getTableId());
    	tmp.writePage(pageToBeFlushed);
    	//Mark as not dirty needed
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
    //TODO
        // some code goes here
        // not necessary for lab1|lab2
    	//UHMMMMMM we don't yet have a supporting data structure?
    
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
    //TODO
        // some code goes here
        // not necessary for lab1
    	PageId pid;
    	do {
    		pid = prior.remove().pid;
    		prio--;
    	} while(!pages.containsKey(pid.hashCode()));
    	pages.remove(pages.get(pid.hashCode()));
    	//WE NEED SOME TYPE OF ENVICTION POLICY CUZ THINGS BE CRAY CRAY
    }
}
