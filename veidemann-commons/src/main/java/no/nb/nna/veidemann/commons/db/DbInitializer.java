package no.nb.nna.veidemann.commons.db;

public interface DbInitializer {
    /**
     * Create or upgrade database.
     *
     * @throws DbException
     */
    void initialize() throws DbException;

    /**
     * Delete database.
     *
     * @throws DbException
     */
    void delete() throws DbException;
}
