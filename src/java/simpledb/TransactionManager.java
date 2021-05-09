package simpledb;

import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionManager {
    private ConcurrentHashMap<PageId, Locks> lockMap;
    private ConcurrentHashMap<TransactionId, Set<TransactionId>> waiterMap; 
    private Set<TransactionId> visit;

    class Locks {     
        public PageId pageid;
        public TransactionId writeLock;
        public Set<TransactionId> readLocks;

        public Locks (PageId pageid) {
            this.pageid = pageid;
            this.writeLock = null;
            this.readLocks = Collections.newSetFromMap(new ConcurrentHashMap<TransactionId, Boolean>());            
        }        

        public boolean isFree() {
            return writeLock == null && readLocks.isEmpty();
        }
    }

    public TransactionManager () {        
        lockMap = new ConcurrentHashMap<> ();
        waiterMap = new ConcurrentHashMap<> ();
        visit = Collections.newSetFromMap(new ConcurrentHashMap<TransactionId, Boolean>());
    }

    private synchronized boolean acquireLockHandler(TransactionId tid, PageId pid, Permissions perm) {
        if (!lockMap.containsKey(pid))
            lockMap.put(pid, new Locks(pid));

        Locks locks = lockMap.get(pid);
        if (perm.equals(Permissions.READ_ONLY)) {
            if (locks.writeLock == null) {
                locks.readLocks.add(tid);
                return true;
            }

            return locks.writeLock.equals(tid);
        } else {
            if (locks.writeLock != null)
                return locks.writeLock.equals(tid);
            if (locks.readLocks.isEmpty() || 
                    (locks.readLocks.size() == 1 && locks.readLocks.contains(tid))) {
                locks.writeLock = tid;
                locks.readLocks.clear();
                return true;
            }

            return false;
        }
    }

    private synchronized void removeWaiters(TransactionId tid) {
        if (waiterMap.containsKey(tid))
            waiterMap.remove(tid);
    }

    private synchronized void addWaiters(TransactionId tid, PageId pid) {
        if (!waiterMap.containsKey(tid)) 
            waiterMap.put(tid, new HashSet<> ());

        Set<TransactionId> waiters = waiterMap.get(tid);
        Locks locks = lockMap.get(pid);

        if (locks == null)
            return;
        if (locks.writeLock != null) {
            waiters.add(locks.writeLock);
            // System.out.println(tid + " -> " + locks.writeLock);
        }
        for (TransactionId t : locks.readLocks) {
            waiters.add(t);
            // System.out.println(tid + " -> " + t);
        }
    }

    private synchronized boolean searchCycle(TransactionId tid, TransactionId ban) {
        if (tid.equals(ban))
            return true;
        if (visit.contains(tid))
            return false;
        visit.add(tid);
        
        Set<TransactionId> waiters = waiterMap.get(tid);        
        if (waiters == null) 
            return false;
        for (TransactionId next : waiters) {
            if (searchCycle(next, ban))
                return true;
        }

        return false;
    }

    private synchronized void detectDeadLock(TransactionId tid) throws TransactionAbortedException {
        visit.clear();
        for (TransactionId t : waiterMap.get(tid)) {
            if (searchCycle(t, tid)) 
                throw new TransactionAbortedException();
        }
    }

    private synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) 
        throws TransactionAbortedException {
        if (acquireLockHandler(tid, pid, perm)) {
            waiterMap.remove(tid);
            return true;
        }
        
        addWaiters(tid, pid);
        detectDeadLock(tid);
        return false;
    }

    public void acquire(TransactionId tid, PageId pid, Permissions perm) 
        throws TransactionAbortedException {
        for (boolean flag = false; !flag; flag = acquireLock(tid, pid, perm));            
    }

    public synchronized void release(TransactionId tid, PageId pid) {
        Locks locks = lockMap.get(pid);
        if (locks != null) {
            if (tid.equals(locks.writeLock))
                locks.writeLock = null;
            else
                locks.readLocks.remove(tid);

            if (locks.isFree()) 
                lockMap.remove(pid);
        }
    }

    public synchronized void release(TransactionId tid) {
        for (PageId pid : lockMap.keySet().toArray(new PageId [lockMap.size()]))
            release(tid, pid);        
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        Locks locks = lockMap.get(pid);
        return locks != null && (locks.readLocks.contains(tid) || (locks.writeLock != null && locks.writeLock.equals(tid)));
    }
}