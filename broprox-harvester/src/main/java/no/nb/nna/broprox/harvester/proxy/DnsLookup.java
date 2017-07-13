/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.broprox.harvester.proxy;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.net.InetAddresses;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentracing.tag.Tags;
import no.nb.nna.broprox.commons.opentracing.OpenTracingWrapper;
import no.nb.nna.broprox.commons.DbAdapter;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import org.littleshoot.proxy.HostResolver;
import org.netpreserve.commons.util.datetime.DateFormat;
import org.netpreserve.commons.util.datetime.Granularity;
import org.netpreserve.commons.util.datetime.VariablePrecisionDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.ARecord;
import org.xbill.DNS.CNAMERecord;
import org.xbill.DNS.Cache;
import org.xbill.DNS.Credibility;
import org.xbill.DNS.DClass;
import org.xbill.DNS.DNAMERecord;
import org.xbill.DNS.Message;
import org.xbill.DNS.Name;
import org.xbill.DNS.NameTooLongException;
import org.xbill.DNS.RRset;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.Record;
import org.xbill.DNS.ResolverConfig;
import org.xbill.DNS.SetResponse;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.Type;

/**
 *
 */
public class DnsLookup implements HostResolver {

    private static final Logger LOG = LoggerFactory.getLogger(DnsLookup.class);

    private static final short DCLASS = DClass.IN;

    private static final short TYPE = Type.A;

    final Cache cache;

    String digestAlgorithm = "SHA-1";

    boolean digestContent = true;

    private final DbAdapter db;

    private final ContentWriterClient contentWriterClient;

    /**
     * If a DNS lookup fails, whether or not to fallback to InetAddress resolution, which may use local 'hosts' files or
     * other mechanisms.
     */
    protected boolean acceptNonDnsResolves = false;

    InetAddress serverInetAddr = null;

    private final SimpleResolver[] resolvers;

    public DnsLookup(final DbAdapter db, final ContentWriterClient contentWriterClient, List<String> dnsServers) {
        this.db = db;
        this.contentWriterClient = contentWriterClient;
        cache = new Cache(DCLASS);
        cache.setMaxCache(24 * 3600); // Cache an answer for a maximum of one day
        cache.setMaxNCache(24 * 3600); // Cache negative answers for one day
        cache.setMaxEntries(500000);

        try {
            if (dnsServers == null || dnsServers.isEmpty()) {
                dnsServers = Arrays.asList(ResolverConfig.getCurrentConfig().servers());
                LOG.info("No DNS server configured.");
            }
            resolvers = new SimpleResolver[dnsServers.size()];
            for (int i = 0; i < dnsServers.size(); i++) {
                LOG.info("Initializing DNS server: " + dnsServers.get(i));
                String[] dnsServer = dnsServers.get(i).split(":");
                resolvers[i] = new SimpleResolver(dnsServer[0]);
                if (dnsServer.length == 2) {
                    int dnsPort = Integer.parseInt(dnsServer[1]);
                    resolvers[i].setPort(dnsPort);
                }
            }
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public InetSocketAddress resolve(String host, int port) throws UnknownHostException {
        OpenTracingWrapper otw = new OpenTracingWrapper("DnsLookup", Tags.SPAN_KIND_CLIENT)
                .addTag("lookup", host + ':' + port)
                .setExtractParentSpanFromGrpcContext(false);
        try {
            return otw.call("resolve", new Resolver(host, port));
        } catch (UnknownHostException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * If a DNS lookup fails, whether or not to fallback to InetAddress resolution, which may use local 'hosts' files or
     * other mechanisms.
     */
    public boolean getAcceptNonDnsResolves() {
        return acceptNonDnsResolves;
    }

    public void setAcceptNonDnsResolves(boolean acceptNonDnsResolves) {
        this.acceptNonDnsResolves = acceptNonDnsResolves;
    }

    /**
     * Whether or not to perform an on-the-fly digest hash of retrieved content-bodies.
     */
    public boolean getDigestContent() {
        return digestContent;
    }

    public void setDigestContent(boolean digest) {
        digestContent = digest;
    }

    /**
     * Which algorithm (for example MD5 or SHA-1) to use to perform an on-the-fly digest hash of retrieved
     * content-bodies.
     */
    public String getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public void setDigestAlgorithm(String digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
    }

    protected void storeDnsRecord(final String host, final State state) throws IOException, NoSuchAlgorithmException {
        ByteBuf payload = Unpooled.buffer();

        // Start the record with a 14-digit date per RFC 2540
        VariablePrecisionDateTime fetchDate = new VariablePrecisionDateTime(state.fetchStart, Granularity.SECOND);
        payload.writeCharSequence(fetchDate.toFormattedString(DateFormat.HERITRIX), StandardCharsets.UTF_8);
        payload.writeByte('\n');

        for (Record r : state.answers) {
            payload.writeCharSequence(r.toString(), StandardCharsets.UTF_8);
            payload.writeByte('\n');
        }

        byte[] buf = new byte[payload.readableBytes()];
        payload.getBytes(payload.readerIndex(), buf);

        CrawlLog.Builder crawlLogBuilder = CrawlLog.newBuilder()
                .setRecordType("response")
                .setRequestedUri("dns:" + host)
                .setDiscoveryPath("P")
                .setStatusCode(1)
                .setFetchTimeStamp(ProtoUtils.odtToTs(state.fetchStart))
                .setIpAddress(state.dnsIp)
                .setContentType("text/dns")
                .setSize(payload.readableBytes());

        // Shall we get a digest on the content downloaded?
        if (digestContent) {
            MessageDigest digest = MessageDigest.getInstance(getDigestAlgorithm());

            String digestString = "sha1:" + new BigInteger(1, digest.digest(buf)).toString(16);
            crawlLogBuilder.setBlockDigest(digestString);
        }

        CrawlLog crawlLog = crawlLogBuilder.build();
        if (db != null) {
            crawlLog = db.addCrawlLog(crawlLog);
        }
        if (contentWriterClient != null) {
            URI uri = contentWriterClient.writeRecord(crawlLog, payload, null);
        }

        LOG.debug("DNS record for {} written", host);
    }

    State lookup(String host) {
        Name name = Name.fromConstantString(host);

        State state = new State();

        if (name.labels() < 2) {
            state.result = Result.UNRECOVERABLE;
            state.setErrorType(Error.NAME_TOO_LONG);
            state.error = "name too short";
            state.done = true;
            return state;
        }

        if (!name.isAbsolute()) {
            try {
                name = Name.concatenate(name, Name.root);
            } catch (NameTooLongException ex) {
                state.setErrorType(Error.NAME_TOO_LONG);
                return state;
            }
        }

        lookup(name, state);

        if (!state.done) {
            switch (state.errorType) {
                case BAD_RESPONSE:
                    state.result = Result.TRY_AGAIN;
                    state.error = state.badresponse_error;
                    state.done = true;
                    break;
                case TIMED_OUT:
                    state.result = Result.TRY_AGAIN;
                    state.error = "timed out";
                    state.done = true;
                    break;
                case NETWORK_ERROR:
                    state.result = Result.TRY_AGAIN;
                    state.error = "network error";
                    state.done = true;
                    break;
                case NXDOMAIN:
                    state.result = Result.HOST_NOT_FOUND;
                    state.done = true;
                    break;
                case REFERRAL:
                    state.result = Result.UNRECOVERABLE;
                    state.error = "referral";
                    state.done = true;
                    break;
                case NAME_TOO_LONG:
                    state.result = Result.UNRECOVERABLE;
                    state.error = "name too long";
                    state.done = true;
                    break;
            }
        }

        return state;

    }

    State lookup(Name name, State state) {
        SetResponse sr = cache.lookupRecords(name, TYPE, Credibility.NORMAL);

        LOG.debug("Cache lookup {}, Response {}", name, sr);

        if (sr.isUnknown() || sr.isDelegation()) {
            // Cache miss
            state.fromCache = false;
        }

        processResponse(name, sr, state);

        if (state.done || state.doneCurrent) {
            return state;
        }

        Record question = Record.newRecord(name, TYPE, DCLASS);
        Message query = Message.newQuery(question);
        Message response = fetch(query, state);

        if (state.errorType == null) {
            sr = cache.addMessage(response);
            if (sr == null) {
                sr = cache.lookupRecords(name, TYPE, Credibility.NORMAL);
            }
            LOG.debug("Queried {}, Response {}", name, sr);
            processResponse(name, sr, state);
        }
        return state;
    }

    private void processResponse(Name name, SetResponse response, State state) {
        if (response.isSuccessful()) {
            RRset[] rrsets = response.answers();
            for (int i = 0; i < rrsets.length; i++) {
                Iterator it = rrsets[i].rrs();
                while (it.hasNext()) {
                    state.answers.add((Record) it.next());
                }
            }
            state.result = Result.SUCCESSFUL;
            state.done = true;
        } else if (response.isNXDOMAIN()) {
            state.setErrorType(Error.NXDOMAIN);
            state.doneCurrent = true;
            if (state.iterations > 0) {
                state.result = Result.HOST_NOT_FOUND;
                state.done = true;
            }
        } else if (response.isNXRRSET()) {
            state.result = Result.TYPE_NOT_FOUND;
            state.answers.clear();
            state.done = true;
        } else if (response.isCNAME()) {
            RRset[] rrsets = response.answers();
            CNAMERecord cname = response.getCNAME();
            state.answers.add(cname);
            follow(cname.getTarget(), name, state);
        } else if (response.isDNAME()) {
            DNAMERecord dname = response.getDNAME();
            state.answers.add(dname);
            try {
                follow(name.fromDNAME(dname), name, state);
            } catch (NameTooLongException e) {
                state.result = Result.UNRECOVERABLE;
                state.error = "Invalid DNAME target";
                state.done = true;
            }
        } else if (response.isDelegation()) {
            // We shouldn't get a referral.  Ignore it.
            // state.setErrorType(Error.REFERRAL);
        }
    }

    private void follow(Name name, Name oldname, State state) {
        state.foundAlias = true;
        state.errorType = null;
        state.iterations++;
        if (state.iterations >= 6 || name.equals(oldname)) {
            state.result = Result.UNRECOVERABLE;
            state.error = "CNAME loop";
            state.done = true;
            return;
        }
        if (state.aliases == null) {
            state.aliases = new ArrayList<>();
        }
        state.aliases.add(oldname);
        lookup(name, state);

    }

    private Message fetch(Message query, State state) {
        Message response = null;
        for (SimpleResolver resolver : resolvers) {
            try {
                response = resolver.send(query);
                state.dnsIp = resolver.getAddress().getAddress().getHostAddress();
                state.errorType = null;
            } catch (IOException e) {
                // A network error occurred.
                if (e instanceof InterruptedIOException) {
                    state.setErrorType(Error.TIMED_OUT);
                } else {
                    state.setErrorType(Error.NETWORK_ERROR);
                }
                continue;
            }
            int rcode = response.getHeader().getRcode();
            if (rcode != Rcode.NOERROR && rcode != Rcode.NXDOMAIN) {
                // The server we contacted is broken or otherwise unhelpful.
                state.setErrorType(Error.BAD_RESPONSE);
                state.badresponse_error = Rcode.string(rcode);
                continue;
            }

            if (!query.getQuestion().equals(response.getQuestion())) {
                // The answer doesn't match the question.  That's not good.
                state.setErrorType(Error.BAD_RESPONSE);
                state.badresponse_error = "response does not match query";
                continue;
            }

            return response;
        }
        return null;
    }

    private class Resolver implements Callable<InetSocketAddress> {

        String host;

        Integer port;

        public Resolver(String host, Integer port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public InetSocketAddress call() throws UnknownHostException {
            // Check if host is already an ip address
            if (InetAddresses.isInetAddress(host)) {
                LOG.debug("Host {} is IP address, return as is");
                return new InetSocketAddress(InetAddresses.forString(host), port);
            }

            State state = lookup(host);

            InetSocketAddress address;

            for (Record r : state.answers) {
                if (r.getType() == Type.A) {
                    ARecord ar = (ARecord) r;
                    address = new InetSocketAddress(ar.getAddress(), port);
                    if (!state.fromCache) {
                        try {
                            storeDnsRecord(host, state);
                        } catch (IOException | NoSuchAlgorithmException ex) {
                            LOG.error("Could not store DNS lookup", ex);
                            throw new RuntimeException(ex);
                        }
                    }
                    LOG.debug("Resolved {} to {}", host, address);
                    return address;
                }
            }

            if (getAcceptNonDnsResolves() || "localhost".equals(host)) {
                // Do lookup that bypasses javadns.
                address = new InetSocketAddress(InetAddress.getByName(host), port);
                LOG.debug("Found address {} for {} using native dns.", address, host);
                return address;
            }

            LOG.error("Could not lookup host {}", host);
            throw new UnknownHostException(host);
        }

    }

    private class State {

        Result result;

        List<Record> answers = new ArrayList<>();

        String error;

        boolean done;

        boolean doneCurrent;

        Error errorType;

        String badresponse_error;

        int iterations;

        boolean foundAlias;

        List<Name> aliases;

        OffsetDateTime fetchStart = OffsetDateTime.now().withOffsetSameInstant(ZoneOffset.UTC);

        boolean fromCache = true;

        String dnsIp;

        @Override
        public String toString() {
            return "State{" + "result=" + result + ", answers=" + answers + ", error=" + error + ", done=" + done
                    + ", doneCurrent=" + doneCurrent + ", errorType=" + errorType
                    + ", badresponse_error=" + badresponse_error + ", iterations=" + iterations
                    + ", foundAlias=" + foundAlias + ", aliases=" + aliases + ", fetchStart=" + fetchStart
                    + ", fromCache=" + fromCache + ", dnsIp=" + dnsIp + '}';
        }

        public void setErrorType(Error errorType) {
            if (this.errorType != null) {
                throw new RuntimeException("CONFLICTING ERROR: " + this.errorType + " <> " + errorType);
            }
            this.errorType = errorType;
        }

    }

    private enum Error {

        NXDOMAIN,
        BAD_RESPONSE,
        NETWORK_ERROR,
        TIMED_OUT,
        NAME_TOO_LONG,
        NAME_TOO_SHORT,
        REFERRAL

    }

    private enum Result {

        SUCCESSFUL,
        UNRECOVERABLE,
        TRY_AGAIN,
        HOST_NOT_FOUND,
        TYPE_NOT_FOUND

    }
}
