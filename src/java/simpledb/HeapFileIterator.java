package simpledb;
import java.util.*;

public class HeapFileIterator implements DbFileIterator {
    private int numPages;
    private int heapFileId;
    private TransactionId transactionId;
    private int i;
    private HeapPageId currentHPI;
    private HeapPage currentHP;
    private Iterator<Tuple> iterator;

    public HeapFileIterator(int np, int hfid, TransactionId tid) {
        numPages=np;
        heapFileId=hfid;
        transactionId=tid;
    }
    @Override
    public void open() throws DbException, TransactionAbortedException {
        i = 0;
        currentHPI = new HeapPageId(heapFileId,i);
        currentHP = (HeapPage)Database.getBufferPool().getPage(transactionId, currentHPI, null);
        iterator = currentHP.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (iterator==null)
            return false;
        else if (iterator.hasNext())
            return true;
        else if (i<numPages){
            nextPage();
            return iterator.hasNext();
        }
        else
            return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (iterator==null)
            throw new NoSuchElementException("Iterator was not opened.");
        if (iterator.hasNext())
            return iterator.next();
        else if (i < numPages) {
            nextPage();
            return iterator.next();
        }
        else throw new NoSuchElementException("There are no tuples left.");
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        if (iterator==null)
            throw new DbException("Iterator was not opened.");
        open();
    }

    @Override
    public void close() {
        i = 0;
        currentHPI = null;
        currentHP = null;
        iterator = null;
    }

    public void nextPage() throws TransactionAbortedException, DbException {
        i++;
        currentHPI = new HeapPageId(heapFileId,i);
        currentHP = (HeapPage) Database.getBufferPool().getPage(transactionId, currentHPI, null);
        iterator = currentHP.iterator();
    }
}
