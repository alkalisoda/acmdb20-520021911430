package simpledb;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

//import javafx.scene.control.Slider;

import java.util.ArrayList;
import java.util.HashSet;

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
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    private int numPages;
    private ConcurrentHashMap<PageId, Page> buffer_pool;
    private LockManager lockManager;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private class Lock{
        private TransactionId tid;
    
        public Lock(TransactionId tid){
            this.tid = tid;
        }
    }
    
    private class LockManager{
        // Locks
        private ConcurrentHashMap<PageId, Lock> locks;
        private ConcurrentHashMap<PageId, TransactionId> XLocks;
        private ConcurrentHashMap<PageId, ConcurrentLinkedDeque<TransactionId>> SLocks;
       
        // Denpendency Transactions
        private ConcurrentHashMap<TransactionId, ConcurrentLinkedDeque<PageId>> Lock_holders;
        private ConcurrentHashMap<TransactionId, ConcurrentLinkedDeque<PageId>> XLock_holders;
        private ConcurrentHashMap<TransactionId, ConcurrentLinkedDeque<TransactionId>> dependencyGraph;
    
    
        public LockManager(){
            locks = new ConcurrentHashMap<>();
            XLocks = new ConcurrentHashMap<>();
            SLocks = new ConcurrentHashMap<>();
    
            Lock_holders = new ConcurrentHashMap<>();
            XLock_holders = new ConcurrentHashMap<>();
            dependencyGraph = new ConcurrentHashMap<>();
        }
    
        /**
         * 
         *  AcquireLock operation
         * 
         */
        private boolean ifholdsLock(TransactionId tid, PageId pid, boolean read){
            // X lock hold by tid
            if (XLocks.containsKey(pid) && tid.equals(XLocks.get(pid))){
                return true;
            }
            // S lock hold by tid
            return read && SLocks.containsKey(pid) && SLocks.get(pid).contains(tid);
        }

        public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) 
        throws TransactionAbortedException{
            // aquire S lock
            if (perm == Permissions.READ_ONLY){
                if (ifholdsLock(tid, pid, true)){
                    return true;
                }
                acquireSLock(tid, pid);
            }
            // aquire X lock
            else if (perm == Permissions.READ_WRITE){
                if (ifholdsLock(tid, pid, false)){
                    return true;
                }
                acquireXLock(tid, pid);
            }
            updateLocks(tid, pid);
            return true;
        }

        private void acquireSLock(TransactionId tid, PageId pid)
        throws TransactionAbortedException{
            // get a lock
            Lock lock = addLock(pid, tid);

            while (true){
                synchronized (lock) {
                    TransactionId hold_tid = XLocks.get(pid);
                    // no others hold the lock
                    boolean nonblock = (hold_tid == null || hold_tid.equals(tid));
                    
                    if (nonblock){
                        removeDependency(tid);
                        addRLocks(pid, tid);
                        return;
                    }
                    ArrayList<TransactionId> hIds = new ArrayList<>();
                    hIds.add(hold_tid);
                    updateDependency(tid, hIds);
                }
            }
        }

        private void acquireXLock(TransactionId tid, PageId pid)
        throws TransactionAbortedException{
            // get a lock
            Lock lock = addLock(pid, tid);

            while (true) {
                synchronized(lock) {
                    ArrayList<TransactionId> hIds = new ArrayList<>();
                    // X lock hold
                    if (XLocks.containsKey(pid)){
                        hIds.add(XLocks.get(pid));
                    }
                    if (SLocks.containsKey(pid)){
                        hIds.addAll(SLocks.get(pid));
                    }

                    boolean nonblock = (hIds.size() == 0) || (hIds.size() == 1 && hIds.get(0).equals(tid));

                    if (nonblock){
                        removeDependency(tid);
                        addRWLocks(pid, tid);
                        return;
                    }
                    updateDependency(tid, hIds);
                }
            }
        }

        private Lock addLock(PageId pid, TransactionId tid){
            Lock lock = new Lock(tid);
            locks.putIfAbsent(pid, lock);
            return locks.get(pid);
        }

        private void addRLocks(PageId pid, TransactionId tid){
            SLocks.putIfAbsent(pid, new ConcurrentLinkedDeque<>());
            SLocks.get(pid).add(tid);
        }

        private void addRWLocks(PageId pid, TransactionId tid){
            XLocks.put(pid, tid);
            XLock_holders.putIfAbsent(tid, new ConcurrentLinkedDeque<>());
            XLock_holders.get(tid).add(pid);
        }

        private void updateLocks(TransactionId tid, PageId pid){
            Lock_holders.putIfAbsent(tid, new ConcurrentLinkedDeque<>());
            Lock_holders.get(tid).add(pid);
        }

        /** 
         * 
         *  Denpendency and deadlock
         * 
         */

        private void updateDependency(TransactionId acquireTid, ArrayList<TransactionId> hIds)
        throws TransactionAbortedException{
            dependencyGraph.putIfAbsent(acquireTid, new ConcurrentLinkedDeque<>());
            boolean flag = false;
            ConcurrentLinkedDeque<TransactionId> neighbor = dependencyGraph.get(acquireTid);
            for (TransactionId hId : hIds){
                // lock holder not in current denpendency graph
                if ((!neighbor.contains(hId)) && (!hId.equals(acquireTid))){
                    flag = true;
                    // add dependency
                    dependencyGraph.get(acquireTid).add(hId);
                }
            }
            if (flag) {
                checkDeadLock(acquireTid, new HashSet<>());
            }
        }

        private void checkDeadLock(TransactionId root, HashSet<TransactionId> traverse)
        throws TransactionAbortedException{
            if (!dependencyGraph.containsKey(root)){
                return;
            }
            for (TransactionId child : dependencyGraph.get(root)){
                if (traverse.contains(child)){
                    // dead lock
                    throw new TransactionAbortedException();
                }
                traverse.add(child);
                checkDeadLock(child, traverse);
                traverse.remove(child);
            }
        }

        private void removeDependency(TransactionId tid){
            synchronized(dependencyGraph){
                dependencyGraph.remove(tid);
                for (TransactionId tid2 : dependencyGraph.keySet()){
                    dependencyGraph.get(tid2).remove(tid);
                }
            }
        }

        /**
         * 
         *  realease lock and locks
         * 
         */


        public boolean holdsLock(TransactionId tid, PageId pid){
            return (ifholdsLock(tid, pid, false) || ifholdsLock(tid,pid,true));
        }

        private void releasePage(TransactionId tid, PageId pid){
            if (this.holdsLock(tid, pid)){
                Lock lock = addLock(pid, tid);
                synchronized(lock){
                    // clear all concerned
                    if (SLocks.containsKey(pid)){
                        SLocks.get(pid).remove(tid);
                    }
                    if (XLocks.containsKey(pid) && XLocks.get(pid).equals(tid)){
                        XLocks.remove(pid);
                    }
                    if (Lock_holders.containsKey(tid)){
                        Lock_holders.get(tid).remove(pid);
                    }
                    if (XLock_holders.containsKey(tid)){
                        XLock_holders.get(tid).remove(pid);
                    }
                }
            }
        }

        public void releaseALL(TransactionId tid){
            if (Lock_holders.containsKey(tid)){
                for (PageId pid : Lock_holders.get(tid)){
                    releasePage(tid, pid);
                }
            }
            if (XLock_holders.containsKey(tid)){
                XLock_holders.remove(tid);
            }
        }

        public ConcurrentHashMap<TransactionId, ConcurrentLinkedDeque<PageId>> getDirtyPages(){
            return XLock_holders;
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        buffer_pool = new ConcurrentHashMap<PageId, Page>();
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
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
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        lockManager.acquireLock(tid, pid, perm);
        if(buffer_pool.containsKey(pid)){
            return buffer_pool.get(pid);
        }
        else{
            if (numPages == buffer_pool.size()){
                evictPage();
            }
            DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbfile.readPage(pid);
            buffer_pool.put(pid, page);
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releasePage(tid, pid);;
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */

    private void restorePage(PageId pid){
        DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = dbfile.readPage(pid);
        buffer_pool.replace(pid, page);
        page.markDirty(false, null);
    }

    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        ConcurrentHashMap<TransactionId, ConcurrentLinkedDeque<PageId>> dirtyHashMap = lockManager.getDirtyPages();

        if (dirtyHashMap.containsKey(tid)){
            for (PageId pid : dirtyHashMap.get(tid)){
                if (commit) {
                    flushPage(pid);
                }
                else {
                    restorePage(pid);
                }
            }
        }
        lockManager.releaseALL(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */


    private void updatebuffer(ArrayList<Page> Plist,TransactionId tid, boolean insert) 
    throws DbException{
        for (Page p : Plist){
            p.markDirty(true, tid);
            if (insert){
                discardPage(p.getId());
            }
            if (buffer_pool.size() == numPages){
                evictPage();
            }
            buffer_pool.put(p.getId(), p);
        }
    }

    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pList = dbFile.insertTuple(tid, t);
        updatebuffer(pList, tid, true);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> pList = dbFile.deleteTuple(tid, t);
        updatebuffer(pList, tid, false);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid : buffer_pool.keySet()){
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        buffer_pool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        if (buffer_pool.containsKey(pid)){
            Page page = buffer_pool.get(pid);
            if (page.isDirty() != null){
                DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                dbfile.writePage(page);
                page.markDirty(false, null);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : buffer_pool.keySet()){
            Page page = buffer_pool.get(pid);
            if (tid.equals(page.isDirty())){
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        assert numPages == buffer_pool.size() : "Buffor Pool is not full";

        PageId pageId = null;

        for (PageId pid : buffer_pool.keySet()){
            Page page = buffer_pool.get(pid);
            // not dirty
            if (page != null && page.isDirty() == null){
                pageId = pid;
                try{
                    flushPage(pageId);
                    discardPage(pageId);
                } catch (IOException e){
                    e.printStackTrace();
                }
                break;
            }
        }

        if (pageId == null){
            throw new DbException("All pages are dirty");
        }

    }

}
