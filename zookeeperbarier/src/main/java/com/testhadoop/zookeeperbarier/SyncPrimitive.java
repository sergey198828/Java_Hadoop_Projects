package com.testhadoop.zookeeperbarier;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

class SyncPrimitive implements Watcher {
	//Actual Zookeeper object
	static ZooKeeper zk = null;
    //Mutex object for synchronization
	static Integer mutex;
	//Root directory in Zookeeper
    String rootName;

    SyncPrimitive(String address)
    throws KeeperException, IOException {
        if(zk == null){
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 3000, this);
                mutex = new Integer(-1);
                System.out.println("Finished starting ZK: " + zk);
        }
    }

    synchronized public void process(WatchedEvent arg0) {
        synchronized (mutex) {
            mutex.notify();
        }		
	}
}

