package com.criteo.nosql.casspoke.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


/**
 * A 'white-box' load balancing policy that allows clients to know/control
 * which server host will execute the given query.
 * <p/>
 * This policy queries nodes in a round-robin fashion. For a given query,
 * the plan try only one host: if it fails, the query fails immediately.
 * <p/>
 * This policy is not datacenter aware and will include every known
 * Cassandra host in its round robin algorithm. It is intended for
 * CassandraRunnerStats
 */
public class WhiteLBPolicy implements LoadBalancingPolicy {

    // TODO: It is clearly NOT thread safe !!!
    public final AtomicReference<Host> theLastTargetedHost = new AtomicReference<>();

    private static final Logger logger = LoggerFactory.getLogger(WhiteLBPolicy.class);

    private final CopyOnWriteArrayList<Host> liveHosts = new CopyOnWriteArrayList<Host>();
    private final AtomicInteger index = new AtomicInteger();

    /**
     * Creates a load balancing policy that picks host to query in a round robin
     * fashion (on all the hosts of the Cassandra cluster).
     */
    public WhiteLBPolicy() {
    }

    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {
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
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {

        // We clone liveHosts because we want a version of the list that
        // cannot change concurrently of the query plan iterator (this
        // would be racy). We use clone() as it don't involve a copy of the
        // underlying array (and thus we rely on liveHosts being a CopyOnWriteArrayList).
        @SuppressWarnings("unchecked")
        final List<Host> hosts = (List<Host>) liveHosts.clone();
        final int startIdx = index.getAndIncrement();

        // Overflow protection; not theoretically thread safe but should be good enough
        if (startIdx > Integer.MAX_VALUE - 10000)
            index.set(0);

        return new AbstractIterator<Host>() {

            private int idx = startIdx;
            private int remaining = 1;

            @Override
            protected Host computeNext() {
                if (remaining <= 0)
                    return endOfData();

                remaining--;
                int c = idx++ % hosts.size();
                if (c < 0)
                    c += hosts.size();
                Host h = hosts.get(c);
                theLastTargetedHost.set(h); // update the last targeted host
                return h;
            }
        };
    }

    @Override
    public void onUp(Host host) {
        liveHosts.addIfAbsent(host);
    }

    @Override
    public void onDown(Host host) {
        liveHosts.remove(host);
    }

    @Override
    public void onAdd(Host host) {
        onUp(host);
    }

    @Override
    public void onRemove(Host host) {
        onDown(host);
    }

    @Override
    public void close() {
        // nothing to do
    }
}