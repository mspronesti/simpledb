package simpledb;
import java.util.*;

public class HeapFileIterator implements DbFileIterator {
    private final int numPages;
    private final int heapFileId;
    private final TransactionId transactionId;
    private int currentPageNumber;
    private HeapPageId currentHPId;
    private HeapPage currentHP;
    private Iterator<Tuple> iterator;

    public HeapFileIterator(int numPages, int heapFileId, TransactionId transactionId) {
        this.numPages = numPages;
        this.heapFileId = heapFileId;
        this.transactionId = transactionId;
    }

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    @Override
    public void open() throws DbException, TransactionAbortedException {
        currentPageNumber = 0;
        currentHPId = new HeapPageId(heapFileId, currentPageNumber);
        currentHP = (HeapPage)Database.getBufferPool().getPage(transactionId, currentHPId, null);
        iterator = currentHP.iterator();
    }

    /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (iterator==null)
            return false;
        else if (iterator.hasNext())
            return true;
        else if (currentPageNumber < numPages){
            advanceToNextPage();
            return iterator.hasNext();
        }
        else
            return false;
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (iterator==null)
            throw new NoSuchElementException("Iterator was not opened.");
        if (iterator.hasNext())
            return iterator.next();
        else if (currentPageNumber < numPages) {
            advanceToNextPage();
            return iterator.next();
        }
        else throw new NoSuchElementException("There are no tuples left.");
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        if (iterator==null)
            throw new DbException("Iterator was not opened.");
        open();
    }

    /**
     * Closes the iterator.
     */
    @Override
    public void close() {
        currentPageNumber = 0;
        currentHPId = null;
        currentHP = null;
        iterator = null;
    }

    /**
     *  Advances to next page, if any
     */
    public void advanceToNextPage() throws TransactionAbortedException, DbException {
        currentPageNumber++;
        currentHPId = new HeapPageId(heapFileId, currentPageNumber);
        currentHP = (HeapPage) Database.getBufferPool().getPage(transactionId, currentHPId, null);
        iterator = currentHP.iterator();
    }
}
