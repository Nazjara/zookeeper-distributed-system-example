package com.nazjara.election;

import com.nazjara.networking.WebClient;
import com.nazjara.networking.WebServer;
import com.nazjara.registry.ServiceRegistry;
import com.nazjara.tf_idf.search.SearchCoordinator;
import com.nazjara.tf_idf.search.SearchWorker;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback {
    private final ServiceRegistry workersServiceRegistry;
    private final ServiceRegistry coordinatorServiceRegistry;
    private final int port;
    private WebServer webServer;

    public OnElectionAction(ServiceRegistry workersServiceRegistry, ServiceRegistry coordinatorServiceRegistry, int port) {
        this.workersServiceRegistry = workersServiceRegistry;
        this.coordinatorServiceRegistry = coordinatorServiceRegistry;
        this.port = port;
    }

    @Override
    public void onElectedToBeLeader() {
        try {
            workersServiceRegistry.unregisterFromCluster();
            workersServiceRegistry.registerForUpdates();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }

        if (webServer != null) {
            webServer.stop();
        }

        SearchCoordinator searchCoordinator = new SearchCoordinator(coordinatorServiceRegistry, new WebClient());
        webServer = new WebServer(port, searchCoordinator);
        webServer.startServer();

        try {
            String currentServerAddress = String.format("http://%s:%d%s", InetAddress.getLocalHost().getCanonicalHostName(), port, searchCoordinator.getEndpoint());

            coordinatorServiceRegistry.registerToCluster(currentServerAddress);
        } catch (UnknownHostException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onWorker() {
        SearchWorker searchWorker = new SearchWorker();
        webServer = new WebServer(port, searchWorker);
        webServer.startServer();

        try {
            String currentServerAddress = String.format("http://%s:%d%s", InetAddress.getLocalHost().getCanonicalHostName(), port, searchWorker.getEndpoint());

            workersServiceRegistry.registerToCluster(currentServerAddress);
        } catch (UnknownHostException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}