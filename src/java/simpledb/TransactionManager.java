package simpledb;

import java.util.*;

public class TransactionManager 
{
    class Lock {
        private final PageId pageid;
        private final Set<TransactionId> readLocks;
        private final Set<TransactionId> writeLocks;
        public Lock(PageId pageid) 
        {
            this.pageid = pageid;
            readLocks = new HashSet<TransactionId>();
            writeLocks = new HashSet<TransactionId>();        
        }

        public PageId getPageId() { return pageid; }
        public Set<TransactionId> getReadLocks() { return readLocks; }
        public Set<TransactionId> getWriteLocks() { return writeLocks; }
        public boolean acquire(TransactionId tid, Permissions perm) {
            synchronized (this) {
                if (perm.equals(Permissions.READ_ONLY)) {
                    if (writeLocks.contains(tid)) return true;
                    if (writeLocks.isEmpty()) {
                        readLocks.add(tid);
                        return true;
                    }
                    return false;
                } else {
                    if (readLocks.size() > 1) return false;
                    if (writeLocks.contains(tid)) return true;
                    if (!writeLocks.isEmpty()) return false;
                    if (readLocks.contains(tid)) {
                        readLocks.remove(tid);
                        writeLocks.add(tid);
                        return true;
                    }
                    if (readLocks.isEmpty()) {
                        writeLocks.add(tid);
                        return true;
                    }
                    return false;
                }
            }
        }
    }

    private final Hashtable<PageId, Lock> lockMap;
    private final Hashtable<TransactionId, HashSet<Lock>> tMap;    
    private static final Random Rng = new Random();

    public TransactionManager ()
    {
        tMap = new Hashtable<TransactionId, HashSet<Lock>>();
        lockMap = new Hashtable<PageId, Lock>();
    }

    public synchronized void acquire(TransactionId tid, PageId pid, Permissions perm) 
        throws TransactionAbortedException
    {
        if (!lockMap.containsKey(pid))
            lockMap.put(pid, new Lock(pid));
        if (!tMap.containsKey(tid))
            tMap.put(tid, new HashSet<Lock>());

        Lock lock = lockMap.get(pid);
        tMap.get(tid).add(lock);

        long start = System.currentTimeMillis();
        while (!lock.acquire(tid, perm)) {
            long limit = 500 + Rng.nextInt(50);
            if (System.currentTimeMillis() - start > limit)             
                throw new TransactionAbortedException();           

            try { wait(50); }
            catch (Exception e) { throw new TransactionAbortedException(); }
        }
    }

    public synchronized void release(TransactionId tid, PageId pid)
    {
        if (tMap.containsKey(tid) && lockMap.containsKey(pid)) {
            Lock lock = lockMap.get(pid);
            if (lock.getWriteLocks().contains(tid)) {
                lock.getWriteLocks().remove(tid);
                tMap.get(tid).remove(lock);
                lockMap.remove(pid);
            } else if (lock.getReadLocks().contains(tid)) {
                lock.getReadLocks().remove(tid);
                tMap.get(tid).remove(lock);
                if (lock.getReadLocks().isEmpty() && lock.getWriteLocks().isEmpty())                     
                    lockMap.remove(pid);                
            }
        }
        notifyAll();
    }

    public synchronized void release(TransactionId tid)
    {
        if (tMap.containsKey(tid)) {
            Set<PageId> pageIds = new HashSet<PageId>();
            for (Lock lock : tMap.get(tid))
                pageIds.add(lock.getPageId());
            for (PageId pid : pageIds)
                release(tid, pid);            
            tMap.remove(tid);
        }
        notifyAll();
    }

    public synchronized Set<PageId> getDirtyPages(TransactionId tid)
    {
        Set<PageId> pageIds = new HashSet<PageId>();
        if (tMap.containsKey(tid))
            for (Lock lock : tMap.get(tid))
                if (lock.getWriteLocks().contains(tid))
                    pageIds.add(lock.getPageId()); 
        return pageIds;
    }

    public synchronized boolean holdsLock(TransactionId tid)
    {
        return tMap.containsKey(tid);
    }
}
