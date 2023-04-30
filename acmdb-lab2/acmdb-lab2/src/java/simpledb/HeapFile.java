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

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableid = pid.getTableId();
        int pgNo = pid.pageNumber();
        int page_size = BufferPool.getPageSize();

        RandomAccessFile f = null;
        try{
            f = new RandomAccessFile(file, "r");
            if ((pgNo + 1) * page_size > f.length()){
                f.close();
                throw new IllegalArgumentException("the page does not exist in this file.");
            }
            byte[] bytes = new byte [page_size];
            f.seek(pgNo * page_size);
            int read = f.read(bytes, 0, page_size);
            if (read != page_size){
                f.close();
                throw new IllegalArgumentException("the page does not exist in this file.");
            }
            HeapPageId pageid = new HeapPageId(tableid, pgNo);
            f.close();
            return new HeapPage(pageid, bytes);
        }catch (IOException e){
            e.printStackTrace();
        }
        finally{
        }
        throw new IllegalArgumentException("the page does not exist in this file.");
        
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pgNo = page.getId().pageNumber();
        if (pgNo > numPages()){
            throw new IllegalArgumentException("the page does not exist in this file.");
        }
        int pageSize = BufferPool.getPageSize();
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        f.seek(pgNo * pageSize);
        byte[] pagedata = page.getPageData();

        f.write(pagedata);
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        double page_size = 1.0 * BufferPool.getPageSize();
        double file_size = 1.0 * file.length();
        int numpage = (int)Math.floor(file_size / page_size);
        return numpage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here

        ArrayList<Page> pageList = new ArrayList<Page>();
        int numpage = numPages();
        for(int i = 0; i < numpage; i++){
            PageId pid = new HeapPageId(getId(), i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

            if (heapPage.getNumEmptySlots() == 0){
                continue;
            }
            heapPage.insertTuple(t);
            pageList.add(heapPage);

            return pageList;
        }

        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file, true));
        
        byte[] emptyData = HeapPage.createEmptyPageData();
        bufferedOutputStream.write(emptyData);
        bufferedOutputStream.close();
        
        PageId pid = new HeapPageId(getId(), numpage - 1);
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        heapPage.insertTuple(t);
        heapPage.markDirty(true, tid);
        pageList.add(heapPage);
        
        return pageList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pageList = new ArrayList<Page>();
        PageId pid = t.getRecordId().getPageId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        pageList.add(heapPage);
        heapPage.markDirty(true, tid);
        return pageList;
        // not necessary for lab1
    }

    private class HeapFileIterator implements DbFileIterator{
        private HeapFile heapFile;
        private TransactionId tid;
        private Iterator<Tuple> tuple_iterator;
        private int page_index;

        public HeapFileIterator(HeapFile file, TransactionId tid){
            this.heapFile = file;
            this.tid = tid;
        }

        public void open() throws DbException, TransactionAbortedException{
            page_index = 0;
            HeapPageId pid = new HeapPageId(heapFile.getId(), page_index);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            tuple_iterator = page.iterator();
        }

        public void close(){
            tuple_iterator = null;
        }

        public boolean hasNext() throws DbException, TransactionAbortedException{
            if (tuple_iterator == null){
                return false;
            }
            else if (tuple_iterator.hasNext()){
                return true;
            }
            else{
                int numpages = heapFile.numPages();
                if (page_index < (numpages -1)){
                    page_index ++;
                    if (page_index >=0 && page_index < numpages){
                        HeapPageId pid = new HeapPageId(heapFile.getId(), page_index);
                        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                        tuple_iterator = page.iterator();
                    }
                    else{
                        throw new DbException("There are problems opening/accessing the database: Invalid index.");
                    }
                    return tuple_iterator.hasNext();
                }else{
                    return false;
                }
            }
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
            if(tuple_iterator == null || ! tuple_iterator.hasNext()){
                throw new NoSuchElementException("Iterator has no next element.");
            }else{
                return tuple_iterator.next();
            }
        }

        public void rewind() throws DbException, TransactionAbortedException{
            close();
            open();
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
}

