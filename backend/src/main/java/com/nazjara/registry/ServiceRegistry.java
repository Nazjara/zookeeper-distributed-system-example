package com.nazjara.registry;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {

    public static final String WORKERS_REGISTRY_ZNODE = "/workers_service_registry";
    public static final String COORDINATORS_REGISTRY_ZNODE = "/coordinators_service_registry";

    private final ZooKeeper zooKeeper;
    private final String serviceRegistryZnode;

    private String currentZnode;
    private List<String> allServiceAddresses;

    public ServiceRegistry(ZooKeeper zooKeeper, String serviceRegistryZnode) {
        this.zooKeeper = zooKeeper;
        this.serviceRegistryZnode = serviceRegistryZnode;
        createServiceRegistryZnode();
    }

    public void registerToCluster(String metadata) throws KeeperException, InterruptedException {
        if (this.currentZnode == null) {
            this.currentZnode = zooKeeper.create(serviceRegistryZnode + "/n_", metadata.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("Registered to service registry: " + currentZnode);
        }
    }

    public void registerForUpdates() {
        try {
            updateAddresses();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized List<String> getAllServiceAddresses() throws KeeperException, InterruptedException {
        if (allServiceAddresses == null) {
            updateAddresses();
        }

        return allServiceAddresses;
    }

    public void unregisterFromCluster() throws KeeperException, InterruptedException {
        if (currentZnode != null && zooKeeper.exists(currentZnode, false) != null) {
            zooKeeper.delete(currentZnode, -1);
            System.out.println("Unregistered from service registry: " + currentZnode);
        }
    }

    private void createServiceRegistryZnode() {
        try {
            if (zooKeeper.exists(serviceRegistryZnode, false) == null) {
                zooKeeper.create(serviceRegistryZnode, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private synchronized void updateAddresses() throws KeeperException, InterruptedException {
        List<String> workerZnodes = zooKeeper.getChildren(serviceRegistryZnode, this);

        List<String> addresses = new ArrayList<>(workerZnodes.size());

        for (String workerZnode : workerZnodes) {
            String workerZnodeFullPath = serviceRegistryZnode + "/" + workerZnode;
            Stat stat = zooKeeper.exists(workerZnodeFullPath, false);

            if (stat == null) {
                continue;
            }

            byte[] address = zooKeeper.getData(workerZnodeFullPath, false, stat);
            addresses.add(new String(address));
        }

        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("The cluster addresses are: " + allServiceAddresses);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            updateAddresses();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}