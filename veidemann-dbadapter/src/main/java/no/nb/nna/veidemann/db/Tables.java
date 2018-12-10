package no.nb.nna.veidemann.db;

import com.google.protobuf.Message;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.CrawlHostGroup;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.api.MessagesProto.CrawledContent;
import no.nb.nna.veidemann.api.MessagesProto.ExtractedText;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.PageLog;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.api.MessagesProto.Screenshot;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;

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
    ALREADY_CRAWLED_CACHE("already_crawled_cache", null),
    CRAWL_ENTITIES("config_crawl_entities", ConfigObject.getDefaultInstance()),
    SEEDS("config_seeds", ConfigObject.getDefaultInstance());

    public final String name;

    public final Message schema;

    Tables(String name, Message schema) {
        this.name = name;
        this.schema = schema;
    }

}
