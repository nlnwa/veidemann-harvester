package no.nb.nna.veidemann.db;

import com.google.protobuf.Message;
import no.nb.nna.veidemann.api.ConfigProto.BrowserConfig;
import no.nb.nna.veidemann.api.ConfigProto.BrowserScript;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlEntity;
import no.nb.nna.veidemann.api.ConfigProto.CrawlHostGroupConfig;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.CrawlScheduleConfig;
import no.nb.nna.veidemann.api.ConfigProto.PolitenessConfig;
import no.nb.nna.veidemann.api.ConfigProto.RoleMapping;
import no.nb.nna.veidemann.api.ConfigProto.Seed;
import no.nb.nna.veidemann.api.MessagesProto.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.CrawlHostGroup;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.api.MessagesProto.CrawledContent;
import no.nb.nna.veidemann.api.MessagesProto.ExtractedText;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus;
import no.nb.nna.veidemann.api.MessagesProto.PageLog;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.api.MessagesProto.Screenshot;

public enum Tables {
    SYSTEM("system", null),
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
    BROWSER_SCRIPTS("config_browser_scripts", BrowserScript.getDefaultInstance()),
    CRAWL_ENTITIES("config_crawl_entities", CrawlEntity.getDefaultInstance()),
    SEEDS("config_seeds", Seed.getDefaultInstance()),
    CRAWL_JOBS("config_crawl_jobs", CrawlJob.getDefaultInstance()),
    CRAWL_CONFIGS("config_crawl_configs", CrawlConfig.getDefaultInstance()),
    CRAWL_SCHEDULE_CONFIGS("config_crawl_schedule_configs", CrawlScheduleConfig.getDefaultInstance()),
    BROWSER_CONFIGS("config_browser_configs", BrowserConfig.getDefaultInstance()),
    POLITENESS_CONFIGS("config_politeness_configs", PolitenessConfig.getDefaultInstance()),
    CRAWL_HOST_GROUP_CONFIGS("config_crawl_host_group_configs", CrawlHostGroupConfig.getDefaultInstance()),
    ROLE_MAPPINGS("config_role_mappings", RoleMapping.getDefaultInstance());

    public final String name;

    public final Message schema;

    private Tables(String name, Message schema) {
        this.name = name;
        this.schema = schema;
    }

}
