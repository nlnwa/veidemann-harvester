package no.nb.nna.veidemann.integrationtests;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import no.nb.nna.veidemann.api.report.v1.ExecuteDbQueryReply;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class QueryObserver implements ClientResponseObserver<ClientCallStreamObserver, ExecuteDbQueryReply> {
    ClientCallStreamObserver requestStream;
    CountDownLatch finishLatch = new CountDownLatch(1);
    List<String> results = new ArrayList<>();
    StatusRuntimeException exception;

    public QueryObserver() {
    }

    @Override
    public void onNext(ExecuteDbQueryReply value) {
        results.add(value.getRecord());
    }

    @Override
    public void onError(Throwable t) {
        if (t instanceof StatusRuntimeException) {
            exception = (StatusRuntimeException) t;
        } else {
            System.out.println(t);
        }
        requestStream.onError(t);
        finishLatch.countDown();
    }

    @Override
    public void onCompleted() {
        finishLatch.countDown();
    }

    public void await() throws InterruptedException {
        finishLatch.await();
    }

    public void cancel() {
        requestStream.cancel("Cancelled by user", null);
    }

    public List<String> getResults() {
        return results;
    }

    public StatusRuntimeException getException() {
        return exception;
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<ClientCallStreamObserver> requestStream) {
        this.requestStream = requestStream;
    }
}
