package org.fusesource.hawtdb.api;


/**
 * Implemented by objects to provides transactional access 
 * to a page file.
 *  
 * @author chirino
 */
public interface TxPageFile {

    /**
     * Creates a new transaction.
     * 
     * The transaction object implements the {@link Paged} interface
     * so it is what allows you access and mutate the page file data.
     * 
     * @return
     */
    public abstract Transaction tx();

    /**
     * Once this method returns, any previously committed transactions 
     * are flushed and to the disk, ensuring that they will not be lost
     * upon failure.
     */
    public abstract void flush();

    /**
     * If the transaction page file is configured to use a worker thread,
     * then this method performs a non-blocking flush otherwise this
     * method blocks until the flush is completed.
     *
     * The specified runnable is executed once the flush completes.
     *
     * @param onComplete
     */
    public void flush(Runnable onComplete);

}