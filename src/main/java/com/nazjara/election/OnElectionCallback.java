package com.nazjara.election;

public interface OnElectionCallback {
    void onElectedToBeLeader();
    void onWorker();
}