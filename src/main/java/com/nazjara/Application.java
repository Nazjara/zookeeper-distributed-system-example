package com.nazjara;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Application implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        Application application = new Application();
        ZooKeeper zooKeeper = application.connectToZookeeper();

        LeaderElection leaderElection = new LeaderElection(zooKeeper);
        leaderElection.volunteerForLeadership();
        leaderElection.reelectLeader();

        WatchersDemo watchersDemo = new WatchersDemo(zooKeeper);
        watchersDemo.watchTargetZnode();

        application.run();
        application.close();

        System.out.println("Exiting the application");
    }

    public ZooKeeper connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        return zooKeeper;
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.None) {
            if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                System.out.println("Successfully connected to ZooKeeper");
            } else {
                synchronized (zooKeeper) {
                    System.out.println("Disconnected from ZooKeeper");
                    zooKeeper.notifyAll();
                }
            }
        }
    }
}