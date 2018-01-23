package no.nb.nna.veidemann.contentwriter.warc;

import org.jwat.warc.WarcFileNaming;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class VeidemannWarcFileNaming implements WarcFileNaming {

    /**
     * <code>DateFormat</code> to the following format 'yyyyMMddHHmmss'.
     */
    protected DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * Prefix component.
     */
    protected String filePrefix;

    /**
     * Date component converted into a human readable string.
     */
    protected String dateStr;

    /**
     * Host name component.
     */
    protected String hostName;

    /**
     * Extension component (including leading ".").
     */
    protected String extension;

    protected final static AtomicInteger sequenceNumber = new AtomicInteger(0);

    /**
     * Construct file naming instance.
     *
     * @param filePrefix prefix or null, will default to "Veidemann"
     * @param hostName   host name or null, if you want to use default local host name
     */
    public VeidemannWarcFileNaming(String filePrefix, String hostName) {
        if (filePrefix != null) {
            this.filePrefix = filePrefix;
        } else {
            this.filePrefix = "Veidemann";
        }
        this.hostName = hostName;
        extension = ".warc";
        dateStr = dateFormat.format(new Date());
    }

    @Override
    public boolean supportMultipleFiles() {
        return true;
    }

    @Override
    public String getFilename(int sequenceNr, boolean bCompressed) {
        String filename = filePrefix + "-" + dateStr
                + "-" + hostName
                + "-" + String.format("%05d", sequenceNumber.getAndIncrement()) + extension;
        if (bCompressed) {
            filename += ".gz";
        }
        return filename;
    }

}
