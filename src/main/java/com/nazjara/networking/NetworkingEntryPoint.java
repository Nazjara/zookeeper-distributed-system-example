package com.nazjara.networking;

import java.util.Arrays;
import java.util.List;

public class NetworkingEntryPoint {

    private static final String WORKER_ADDRESS_1 = "http://localhost:8081/task";
    private static final String WORKER_ADDRESS_2 = "http://localhost:8082/task";

    public static void main(String[] args) {
        Aggregator aggregator = new Aggregator();
        String task1 = "10,200";
        String task2 = "123456789,1000000000000000000,700000000000001234567";

        List<String> results = aggregator.sendTasksToWorkers(Arrays.asList(WORKER_ADDRESS_1, WORKER_ADDRESS_2),
                Arrays.asList(task1, task2));

        results.forEach(System.out::println);
    }
}
