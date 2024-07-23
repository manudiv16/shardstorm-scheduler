import pykka
from pysyncobj import SyncObj, SyncObjConf
from pysyncobj.batteries import ReplLockManager, ReplDict
import sys
from lockqueue import QueueLockManager
import time


from pysyncobj.node import TCPNode


class AddNodeActor(pykka.ThreadingActor):
    def __init__(self, repObj: SyncObj):
        super().__init__()
        self.repObj = repObj
        self.file_path = "hosts.txt"
        self.self_node = self.repObj.selfNode
        self.nodes = self.repObj.otherNodes.copy()
        self.previous_content = self.read_file()

    def read_file(self):
        try:
            with open(self.file_path, "r") as file:
                return file.readlines()
        except FileNotFoundError:
            return []

    def discover_node(self):
        while True:
            lines = self.read_file()
            file_nodes = {TCPNode(line.strip()) for line in lines}
            self_nodes = self.nodes
            add_nodes = file_nodes - self_nodes - {self.self_node}
            remove_nodes = self_nodes - file_nodes - {self.self_node}
            for node in add_nodes:
                self.repObj.addNodeToCluster(node)
                self.nodes.add(node)
            for node in remove_nodes:
                self.repObj.removeNodeFromCluster(node)
                self.nodes.remove(node)

            time.sleep(3)


class ConsumerActor(pykka.ThreadingActor):
    def __init__(self, lock: QueueLockManager):
        super().__init__()
        self.lock = lock

    def consume(self):
        while True:
            self.lock.consume()


class TestQueue(pykka.ThreadingActor):
    def __init__(self, lock: QueueLockManager):
        super().__init__()
        self.lock = lock

    def put(self, item):
        print("---------------------------------")
        print(f"Putting item {item}")
        print("---------------------------------")
        self.lock.put(item)

    def print(self):
        print("---------------------------------")
        print("Printing queue locks")
        self.lock.printQueue()
        print("---------------------------------")

    def get_node_ids(self):
        print("---------------------------------")
        print("Getting node ids")
        result = self.lock.get_self_lock()
        print("---------------------------------")
        return result

    def get(self):
        print("---------------------------------")
        print("Getting")
        response = self.lock.consume()
        # time.sleep(10)
        # is_acquired = self.lock.isAcquired("a")
        print("---------------------------------")
        if response:
            print(f"Acquired lock: {response}")
        else:
            print("Failed to acquire lock")
        # print(f"Is acquired: {is_acquired}")
        return response


def main():
    try:

        if len(sys.argv) < 3:
            print(
                "Usage: %s selfHost:port partner1Host:port partner2Host:port ..."
                % sys.argv[0]
            )
            sys.exit(-1)
        selfAddr = f"localhost:{int(sys.argv[1])}"
        partners = ["localhost:%d" % int(p) for p in sys.argv[2:]]
        lockFillKv = ReplDict()
        lockManagerEntries = ReplLockManager(autoUnlockTime=1)

        queueLockManager = QueueLockManager(autoUnlockTime=1)

        syncObj = SyncObj(
            selfAddr,
            partners,
            consumers=[lockManagerEntries, lockFillKv, queueLockManager],
            conf=SyncObjConf(dynamicMembershipChange=True),
        )

        actor = AddNodeActor.start(syncObj).proxy()
        test = TestQueue.start(queueLockManager).proxy()
        consume_actor = ConsumerActor.start(queueLockManager).proxy()

        # test2 = TestQueue2.start(queueLockManager).proxy()

        actor.discover_node()
        consume_actor.consume()

        while True:
            print("Commands: get_node_ids, put, print, get")

            def get_input(v):
                if sys.version_info >= (3, 0):
                    return input(v)
                else:
                    return raw_input(v)

            cmd = get_input(">> ").split()
            if not cmd:
                continue
            elif cmd[0] == "print":
                result = test.print()
                time.sleep(1)
            elif cmd[0] == "get_node_ids":
                result = test.get_node_ids()
                print(result.get())
                time.sleep(1)
            elif cmd[0] == "get":
                result = test.get()
                print(result.get())
                time.sleep(1)
            elif cmd[0] == "put":
                test.put(cmd[1])
                time.sleep(1)

    except Exception as e:
        print("Error: %s" % e)
    finally:
        pykka.ActorRegistry.stop_all()


if __name__ == "__main__":
    main()
