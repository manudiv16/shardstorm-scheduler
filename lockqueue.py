import threading
import weakref
import time
import socket
import os
from pysyncobj import SyncObjConsumer, replicated
from collections import OrderedDict
from returns.result import Result, Success, Failure


class _QueueLockManagerImpl(SyncObjConsumer):
    def __init__(self, autoUnlockTime):
        super(_QueueLockManagerImpl, self).__init__()
        self.__locks = {}
        self.__queue_lock = OrderedDict()
        self.__autoUnlockTime = autoUnlockTime

    @replicated
    def put(self, lockID: str) -> Result[str, str]:
        if lockID in self.__locks:
            return Failure("Lock already exists")
        self.__queue_lock[lockID] = None
        self.__locks[lockID] = (None, 0)
        return Success[lockID]

    @replicated
    def get(self, clientID, currentTime) -> str:
        if len(self.__queue_lock) == 0:
            return ""
        lockId: str
        lockId, _ = self.__queue_lock.popitem(last=False)
        self.__locks[lockId] = (clientID, currentTime)
        return lockId

    @replicated
    def prolongate(self, clientID, currentTime):
        for lockID in list(self.__locks):
            lockClientID, lockTime = self.__locks[lockID]

            if currentTime - lockTime > self.__autoUnlockTime:
                del self.__locks[lockID]
                self.__queue_lock[lockID] = None
                continue
            if lockClientID == clientID:
                self.__locks[lockID] = (clientID, currentTime)

    @replicated
    def release(self, lockID, clientID):
        existingLock = self.__locks.get(lockID, None)
        if existingLock and existingLock[0] == clientID:
            del self.__locks[lockID]
            del self.__queue_lock[lockID]

    def printQueue(self):
        print(self.__locks)
        print(self.__queue_lock)

    def get_self_lock(self, clientID):
        list_lockIds = [
            lockID
            for lockID in list(self.__locks)
            if self.__locks[lockID][0] == clientID
        ]
        return list_lockIds

    def isAcquired(self, lockID, clientID, currentTime):
        existingLock = self.__locks.get(lockID, None)
        if existingLock:
            if existingLock[0] == clientID:
                if currentTime - existingLock[1] < self.__autoUnlockTime:
                    return True
        return False


class QueueLockManager(object):

    def __init__(self, autoUnlockTime, selfID=None):
        """Replicated Lock Manager. Allow to acquire / release distributed locks.

        :param autoUnlockTime: lock will be released automatically
            if no response from holder for more than autoUnlockTime seconds
        :type autoUnlockTime: float
        :param selfID: (optional) - unique id of current lock holder.
        :type selfID: str
        """
        self.__lockImpl = _QueueLockManagerImpl(autoUnlockTime)
        if not selfID:
            selfID = "%s:%d:%d" % (socket.gethostname(), os.getpid(), id(self))
        self.__selfID = selfID
        self.__autoUnlockTime = autoUnlockTime
        self.__mainThread = threading.current_thread()
        self.__initialised = threading.Event()
        self.__destroying = False
        self.__lastProlongateTime = 0
        self.__thread = threading.Thread(
            target=QueueLockManager._autoAcquireThread, args=(weakref.proxy(self),)
        )
        self.__thread.start()
        while not self.__initialised.is_set():
            pass

    def _consumer(self):
        return self.__lockImpl

    def destroy(self):
        """Destroy should be called before destroying ReplLockManager"""
        self.__destroying = True

    def put(self, lockID):
        """Put lock into queue.

        :param lockID: unique lock identifier.
        :type lockID: str
        """
        self.__lockImpl.put(lockID)

    def printQueue(self):
        """Print all locks in queue."""
        self.__lockImpl.printQueue()

    def _autoAcquireThread(self):
        self.__initialised.set()
        try:
            while True:
                if not self.__mainThread.is_alive():
                    break
                if self.__destroying:
                    break
                time.sleep(0.1)
                if (
                    time.time() - self.__lastProlongateTime
                    < float(self.__autoUnlockTime) / 4.0
                ):
                    continue
                syncObj = self.__lockImpl._syncObj
                if not syncObj:
                    continue
                if syncObj._getLeader():
                    self.__lastProlongateTime = time.time()
                    self.__lockImpl.prolongate(self.__selfID, time.time())
        except ReferenceError:
            pass

    def consume(self, callback=None, sync=False, timeout=None):
        attemptTime = time.time()
        acquireLockID = self.__lockImpl.get(
            self.__selfID,
            attemptTime,
            callback=callback,
            sync=True,
            timeout=timeout,
        )
        acquireTime = time.time()
        if acquireLockID:
            if acquireTime - attemptTime > self.__autoUnlockTime / 2.0:
                self.__lockImpl.release(acquireLockID, self.__selfID, sync=sync)
        return acquireLockID

    def get_self_lock(self):
        """Get all locks acquired by ourselves.

        :return: list of lock identifiers.
        :rtype: list[str]
        """
        return self.__lockImpl.get_self_lock(self.__selfID)

    # def isAcquired(self, lockID):
    #     """Check if lock is acquired by ourselves.

    #     :param lockID: unique lock identifier.
    #     :type lockID: str
    #     :return True if lock is acquired by ourselves.
    #     """
    #     return self.__lockImpl.isAcquired(lockID, self.__selfID, time.time())

    # def release(self, lockID, callback=None, sync=False, timeout=None):
    #     """
    #     Release previously-acquired lock.

    #     :param lockID:  unique lock identifier.
    #     :type lockID: str
    #     :param sync: True - to wait until lock is released or failed to release.
    #     :type sync: bool
    #     :param callback: if sync is False - callback will be called with operation result.
    #     :type callback: func(opResult, error)
    #     :param timeout: max operation time (default - unlimited)
    #     :type timeout: float
    #     """
    #     self.__lockImpl.release(
    #         lockID, self.__selfID, callback=callback, sync=sync, timeout=timeout
    #     )
