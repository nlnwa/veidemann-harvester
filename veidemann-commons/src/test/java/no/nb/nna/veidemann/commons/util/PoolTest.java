package no.nb.nna.veidemann.commons.util;

import no.nb.nna.veidemann.commons.util.Pool.Lease;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class PoolTest {

    @Test
    public void lease() throws InterruptedException, TimeoutException, ExecutionException {
        AtomicInteger idx = new AtomicInteger(0);

        Pool<ExpensiveObject> pool = new Pool<>(4, () -> new ExpensiveObject(idx.getAndIncrement()), null, null);

        // Check that we can lease all objects in pool
        Lease<ExpensiveObject> l1 = pool.lease();
        assertThat(l1.getObject().idx).isEqualTo(0);

        Lease<ExpensiveObject> l2 = pool.lease();
        assertThat(l2.getObject().idx).isEqualTo(1);

        Lease<ExpensiveObject> l3 = pool.lease();
        assertThat(l3.getObject().idx).isEqualTo(2);

        Lease<ExpensiveObject> l4 = pool.lease();
        assertThat(l4.getObject().idx).isEqualTo(3);

        // Close one lease and check that we can't acces the object
        l3.close();
        assertThatIllegalStateException()
                .isThrownBy(() -> { l3.getObject(); });

        // Check that we can lease the closed object
        Lease<ExpensiveObject> l5 = pool.lease();
        assertThat(l5.getObject().idx).isEqualTo(2);

        // Check that we cannot lease object from exhausted pool.
        long start = System.currentTimeMillis();
        assertThatExceptionOfType(TimeoutException.class)
                .isThrownBy(() -> { pool.lease(500, TimeUnit.MILLISECONDS); });
        assertThat(System.currentTimeMillis() - start).isBetween(500L, 550L);

        Future<Lease<ExpensiveObject>> l = ForkJoinPool.commonPool().submit((Callable<Lease<ExpensiveObject>>) pool::lease);
        Thread.sleep(500);
        assertThat(l.isDone()).isFalse();
        l1.close();
        assertThat(l.get().getObject().idx).isEqualTo(0);
        assertThat(l.isDone()).isTrue();

    }

    class ExpensiveObject {
        final int idx;

        public ExpensiveObject(int idx) {
            this.idx = idx;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("ExpensiveObject{");
            sb.append("idx=").append(idx);
            sb.append('}');
            return sb.toString();
        }
    }
}