package com.nazjara;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class WatchersDemo implements Watcher {

    private ZooKeeper zookeeper;
    private static final String TARGET_ZNODE = "/target_znode";

    public WatchersDemo(ZooKeeper zookeeper) {
        this.zookeeper = zookeeper;
    }

    public void watchTargetZnode() throws KeeperException, InterruptedException {
        Stat stat = zookeeper.exists(TARGET_ZNODE, this);

        if (stat == null) {
            return;
        }

        byte[] data = zookeeper.getData(TARGET_ZNODE, this, stat);
        List<String> children = zookeeper.getChildren(TARGET_ZNODE, this);

        System.out.println("Data: " + new String(data) + " children: " + children);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case NodeDeleted:
                System.out.println(TARGET_ZNODE + " was deleted");
                break;
            case NodeCreated:
                System.out.println(TARGET_ZNODE + " was created");
                break;
            case NodeDataChanged:
                System.out.println(TARGET_ZNODE + " data was changed");
                break;
            case NodeChildrenChanged:
                System.out.println(TARGET_ZNODE + " children were changed");
                break;
        }

        try {
            watchTargetZnode();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}