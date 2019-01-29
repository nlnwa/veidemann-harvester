package no.nb.nna.veidemann.db;

import com.google.protobuf.Message;
import no.nb.nna.veidemann.api.MessagesProto.ExtractedText;
import no.nb.nna.veidemann.api.MessagesProto.Screenshot;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.contentwriter.v1.CrawledContent;
import no.nb.nna.veidemann.api.contentwriter.v1.StorageRef;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.CrawlLog;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.PageLog;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;

public enum Tables {
    SYSTEM("system", null),
    CONFIG("config", ConfigObject.getDefaultInstance()),
    LOCKS("locks", null),
    CRAWL_LOG("crawl_log", CrawlLog.getDefaultInstance()),
    PAGE_LOG("page_log", PageLog.getDefaultInstance()),
    CRAWLED_CONTENT("crawled_content", CrawledContent.getDefaultInstance()),
    EXTRACTED_TEXT("extracted_text", ExtractedText.getDefaultInstance()),
    URI_QUEUE("uri_queue", QueuedUri.getDefaultInstance()),
    SCREENSHOT("screenshot", Screenshot.getDefaultInstance()),
    EXECUTIONS("executions", CrawlExecutionStatus.getDefaultInstance()),
    JOB_EXECUTIONS("job_executions", JobExecutionStatus.getDefaultInstance()),
    CRAWL_HOST_GROUP("crawl_host_group", CrawlHostGroup.getDefaultInstance()),
    CRAWL_ENTITIES("config_crawl_entities", ConfigObject.getDefaultInstance()),
    SEEDS("config_seeds", ConfigObject.getDefaultInstance()),
    STORAGE_REF("storage_ref", StorageRef.getDefaultInstance());

    public final String name;

    public final Message schema;

    Tables(String name, Message schema) {
        this.name = name;
        this.schema = schema;
    }

}
