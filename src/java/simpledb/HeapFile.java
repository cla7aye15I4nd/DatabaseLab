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
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        Page page = null;
        byte[] buf = new byte[BufferPool.getPageSize()];

        try {
            RandomAccessFile f = new RandomAccessFile(getFile(), "r");
                
            f.seek(pid.pageNumber() * BufferPool.getPageSize());
            f.read(buf, 0, buf.length);
            page = new HeapPage((HeapPageId) pid, buf);
        } catch (IOException e) { /* EMPTY */ } 
        
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId _tid) {
        DbFileIterator iterator = new DbFileIterator() {
            private int i;
            private TransactionId tid = _tid;
            private Iterator<Tuple> iterTuple;

            private Iterator<Tuple> getTupleIterator() throws DbException, 
                TransactionAbortedException {
                return ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_ONLY)).iterator();
            }

            @Override
            public void open() throws DbException, 
                TransactionAbortedException {
                i = 0;
                iterTuple = getTupleIterator();
            }
            
            @Override
            public boolean hasNext() throws DbException, 
                TransactionAbortedException {
                if (iterTuple == null)
                    return false;
                if (iterTuple.hasNext())
                    return true;
                if (i + 1 >= numPages())
                    return false;

                i++;
                iterTuple = getTupleIterator();
                return iterTuple.hasNext();
            }

            @Override
            public Tuple next() throws DbException, 
                TransactionAbortedException, NoSuchElementException {
                if (hasNext())
                    return iterTuple.next();
                
                throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, 
                TransactionAbortedException {
                this.open();
            }

            @Override
            public void close() {
                iterTuple = null;
            }
        };
        return iterator;
    }

}

