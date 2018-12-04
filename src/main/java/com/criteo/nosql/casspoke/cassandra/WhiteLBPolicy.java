package com.criteo.nosql.casspoke.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;


/**
 * A 'white-box' load balancing policy that allows clients to know/control
 * which server host will execute the given query.
 * <p/>
 * This policy queries nodes in a round-robin fashion. For a given query,
 * the plan try only one host: if it fails, the query fails immediately.
 * <p/>
 * This policy is not datacenter aware and will include every known
 * Cassandra host in its round robin algorithm.
 */
public class WhiteLBPolicy implements LoadBalancingPolicy {

    private static final Logger logger = LoggerFactory.getLogger(WhiteLBPolicy.class);

    // TODO: It is clearly NOT thread safe !!!
    public final AtomicReference<Host> theLastTargetedHost = new AtomicReference<>();

    private final List<Host> liveHosts = new ArrayList<>();
    private final AtomicInteger index = new AtomicInteger();
    private final Consumer<Host> onHostRemoved;

    /**
     * Creates a load balancing policy that picks host to query in a round robin
     * fashion (on all the hosts of the Cassandra cluster).
     */
    public WhiteLBPolicy(Consumer<Host> onHostRemoved) {
        this.onHostRemoved = onHostRemoved;
    }

    @Override
    synchronized public void init(Cluster cluster, Collection<Host> hosts) {
        this.liveHosts.addAll(hosts);
        this.index.set(0);
    }

    /**
     * Return the HostDistance for the provided host.
     * <p/>
     * This policy consider all nodes as local.
     *
     * @param host the host of which to return the distance of.
     * @return the HostDistance to {@code host}.
     */
    @Override
    public HostDistance distance(Host host) {
        return HostDistance.LOCAL;
    }

    /**
     * Returns the host to use for a new query.
     * <p/>
     * The returned plan will try one known host of the cluster. Upon each
     * call to this method, the {@code i}th host of the plans returned will cycle
     * over all the hosts of the cluster in a round-robin fashion.
     *
     * @param loggedKeyspace the keyspace currently logged in on for this
     *                       query.
     * @param statement      the query for which to build the plan.
     * @return a new query plan, i.e. an iterator indicating which host to
     * try first for querying, which one to use as failover, etc...
     */
    @Override
    synchronized public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {

        final int startIdx = index.getAndIncrement();
        // Overflow protection
        if (startIdx > Integer.MAX_VALUE - 10000)
            index.set(0);

        final Host host = liveHosts.get(startIdx % liveHosts.size());
        theLastTargetedHost.set(host); // TODO: not thread safe !!!
        return Collections.singleton(host).iterator();
    }

    @Override
    synchronized public void onUp(Host host) {
        if (!liveHosts.contains(host)) {
            liveHosts.add(host);
        }
    }

    @Override
    synchronized public void onDown(Host host) {
        liveHosts.remove(host);
    }

    @Override
    public void onAdd(Host host) {
        onUp(host);
    }

    @Override
    public void onRemove(Host host) {
        onDown(host);
        onHostRemoved.accept(host);
    }

    @Override
    public void close() {
        // nothing to do
    }
}