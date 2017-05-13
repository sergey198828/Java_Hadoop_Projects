package com.testhadoop.zookeeperbarier;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Barier extends SyncPrimitive {
	int size;
	String nodeName;
    /**
     * Barrier constructor
     *
     * @param address
     * @param name
     * @param size
     */	
    Barier(String address, String rootName, int size, String nodeName)throws KeeperException, InterruptedException, IOException {
        super(address);
        this.rootName = rootName;
        this.size = size;

        // Create barrier node
        if (zk != null) {
                Stat s = zk.exists(rootName, false);
                if (s == null) {
                    zk.create(rootName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
        }

        // My node name
        if(nodeName==null){
        	this.nodeName = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
        }else{
        	this.nodeName = nodeName;
        }
    }
    /**
     * Join barrier
     *
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    boolean enter() throws KeeperException, InterruptedException{
        zk.create(rootName + "/" + nodeName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(rootName, true);

                if (list.size() < size) {
                    mutex.wait();
                } else {
                    return true;
                }
            }
        }
    }
    /**
     * Wait until all reach barrier
     *
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    boolean leave() throws KeeperException, InterruptedException{
        zk.delete(rootName + "/" + nodeName, 0);
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(rootName, true);
                if (list.size() > 0) {
                    mutex.wait();
                } else {
                    Stat s = zk.exists(rootName, false);
                    if (s != null) {
                		zk.delete(rootName, 0);
                	}
                    return true;
                }
             }
         }
    }
}
