package no.nb.nna.veidemann.commons.client;

import io.grpc.Context;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.Callable;

public class GrpcUtil {
    private GrpcUtil() {
    }

    public static <V> V forkedCall(Callable<V> callable) {
        try {
            return Context.current().fork().call(callable);
        } catch (Exception e) {
            if (e instanceof StatusRuntimeException) {
                throw (StatusRuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }
}
