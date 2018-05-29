package no.nb.nna.veidemann.controller;

import org.junit.Test;

import static org.junit.Assert.*;

public class ReportServiceTest {

    @Test
    public void executeJsQuery() {
        ReportService service = new ReportService(null);
        service.executeJsQuery("", null);
    }
}