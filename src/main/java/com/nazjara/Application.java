package com.nazjara;

import com.nazjara.election.LeaderElection;
import com.nazjara.election.OnElectionAction;
import com.nazjara.election.OnElectionCallback;
import com.nazjara.registry.ServiceRegistry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Application implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final int DEFAULT_PORT = 8080;
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        int currentServerPort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;

        Application application = new Application();
        ZooKeeper zooKeeper = application.connectToZookeeper();

        ServiceRegistry workersServiceRegistry = new ServiceRegistry(zooKeeper);
        ServiceRegistry coordinatorServiceRegistry = new ServiceRegistry(zooKeeper);

        OnElectionCallback onElectionCallback = new OnElectionAction(workersServiceRegistry,coordinatorServiceRegistry, currentServerPort);

        LeaderElection leaderElection = new LeaderElection(zooKeeper, onElectionCallback);
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