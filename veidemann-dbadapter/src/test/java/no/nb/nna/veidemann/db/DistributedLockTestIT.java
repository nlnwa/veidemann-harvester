package no.nb.nna.veidemann.db;

import no.nb.nna.veidemann.commons.db.DbConnectionException;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbQueryException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.db.DistributedLock.NotOwnerException;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class DistributedLockTestIT {
    private static RethinkDbConnection db;

    @BeforeClass
    public static void init() throws DbException {
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));

        if (!DbService.isConfigured()) {
            CommonSettings settings = new CommonSettings();
            DbService.configure(new CommonSettings()
                    .withDbHost(dbHost)
                    .withDbPort(dbPort)
                    .withDbName("veidemann")
                    .withDbUser("admin")
                    .withDbPassword(""));
        }

        try {
            DbService.getInstance().getDbInitializer().delete();
        } catch (DbException e) {
            if (!e.getMessage().matches("Database .* does not exist.")) {
                throw e;
            }
        }
        DbService.getInstance().getDbInitializer().initialize();
        db = ((RethinkDbInitializer) DbService.getInstance().getDbInitializer()).getDbConnection();
    }

    @AfterClass
    public static void shutdown() {
        DbService.getInstance().close();
    }

    @Test
    public void lockUnlock() throws DbConnectionException, DbQueryException, InterruptedException {
        String key = UUID.randomUUID().toString();
        DistributedLock lock1 = new DistributedLock(db, key, 2);
        lock1.lock();
        try {
            Thread.sleep(10);
        } finally {
            lock1.unlock();
        }
    }

    @Test
    public void doubleLockUnlock() throws DbConnectionException, DbQueryException, InterruptedException {
        String key = UUID.randomUUID().toString();
        DistributedLock lock1 = new DistributedLock(db, key, 2);
        lock1.lock();
        try {
            lock1.lock();
            Thread.sleep(10);
        } finally {
            lock1.unlock();
        }
    }

    @Test
    public void lockDoubleUnlock() throws DbConnectionException, DbQueryException, InterruptedException {
        String key = UUID.randomUUID().toString();
        DistributedLock lock1 = new DistributedLock(db, key, 2);
        lock1.lock();
        try {
            Thread.sleep(10);
        } finally {
            lock1.unlock();
        }
        lock1.unlock();
    }

    @Test
    public void lockAndWaitForTimeout() throws DbConnectionException, DbQueryException, InterruptedException {
        String key = UUID.randomUUID().toString();
        DistributedLock lock1 = new DistributedLock(db, key, 1);
        DistributedLock lock2 = new DistributedLock(db, key, 1);
        lock1.lock();
        try {
            lock2.lock();
        } finally {
            assertThatExceptionOfType(NotOwnerException.class).isThrownBy(() -> lock1.unlock())
                    .withMessage("Lock for '" + key + "' owned by instance '" + lock2.getInstanceId() + "'");
        }
    }

    @Test
    public void tryLock() throws DbQueryException, DbConnectionException {
        String key = UUID.randomUUID().toString();
        DistributedLock lock1 = new DistributedLock(db, key, 2);
        DistributedLock lock2 = new DistributedLock(db, key, 2);
        assertThat(lock1.tryLock()).isTrue();
        assertThat(lock1.tryLock()).isTrue();
        try {
            assertThat(lock2.tryLock()).isFalse();
        } finally {
            lock1.unlock();
        }
        assertThat(lock2.tryLock()).isTrue();
        lock2.unlock();
    }

    @Test
    public void tryLockTimeout() throws DbConnectionException, DbQueryException, InterruptedException {
        String key = UUID.randomUUID().toString();
        DistributedLock lock1 = new DistributedLock(db, key, 1);
        DistributedLock lock2 = new DistributedLock(db, key, 1);
        assertThat(lock1.tryLock(500, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(lock1.tryLock(500, TimeUnit.MILLISECONDS)).isTrue();
        try {
            assertThat(lock2.tryLock(500, TimeUnit.MILLISECONDS)).isFalse();
        } finally {
            lock1.unlock();
        }
        assertThat(lock2.tryLock(500, TimeUnit.MILLISECONDS)).isTrue();
        lock2.unlock();


        assertThat(lock1.tryLock(500, TimeUnit.MILLISECONDS)).isTrue();
        try {
            assertThat(lock2.tryLock(3, TimeUnit.SECONDS)).isTrue();
        } finally {
            assertThatExceptionOfType(NotOwnerException.class).isThrownBy(() -> lock1.unlock())
                    .withMessage("Lock for '" + key + "' owned by instance '" + lock2.getInstanceId() + "'");
        }
        assertThat(lock2.tryLock(500, TimeUnit.MILLISECONDS)).isTrue();
        lock2.unlock();
    }

    @Test
    public void unlock() {
    }
}