package com.nazjara.election;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {

    private static final String ELECTION_NAMESPACE = "/election";
    private final ZooKeeper zookeeper;
    private String currentZnodeName;
    private final OnElectionCallback onElectionCallback;

    public LeaderElection(ZooKeeper zookeeper, OnElectionCallback onElectionCallback) {
        this.zookeeper = zookeeper;
        this.onElectionCallback = onElectionCallback;
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zookeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("Znode name " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void reelectLeader() throws KeeperException, InterruptedException {
        Stat predecessorStat = null;
        String predecessorZnodeName = "";

        while (predecessorStat == null) {
            List<String> children = zookeeper.getChildren(ELECTION_NAMESPACE, false);

            Collections.sort(children);
            String smallestChild = children.get(0);

            if (smallestChild.endsWith(currentZnodeName)) {
                System.out.println("Current znode " + currentZnodeName + " is a leader");

                onElectionCallback.onElectedToBeLeader();
                return;
            } else {
                System.out.println("Current znode " + currentZnodeName + " is not a leader.");

                onElectionCallback.onWorker();

                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZnodeName = children.get(predecessorIndex);
                predecessorStat = zookeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }

        System.out.println("Watching znode " + predecessorZnodeName);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
            try {
                reelectLeader();
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}