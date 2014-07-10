/*
 * Copyright (c) 2013 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
/**
 * @file DijkstraImplementation.java
 *
 *
 * @brief Implementation of a routing engine using dijkstra. Implementation of
 * dijkstra come from Jung2 library
 *
 */
package org.opendaylight.controller.routing.dijkstrav2_implementation.internal;

import edu.uci.ics.jung.algorithms.shortestpath.DijkstraShortestPath;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.SparseMultigraph;
import edu.uci.ics.jung.graph.util.EdgeType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.collections15.Transformer;
import org.opendaylight.controller.clustering.services.IClusterContainerServices;
import org.opendaylight.controller.sal.core.Bandwidth;
import org.opendaylight.controller.sal.core.ConstructionException;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.core.Path;
import org.opendaylight.controller.sal.core.Property;
import org.opendaylight.controller.sal.core.UpdateType;
import org.opendaylight.controller.sal.routing.IDijkstraInterface;
import org.opendaylight.controller.sal.routing.IListenRoutingUpdates;
import org.opendaylight.controller.sal.routing.IListenRoutingUpdatesWrapper;
import org.opendaylight.controller.sal.topology.TopoEdgeUpdate;
import org.opendaylight.controller.switchmanager.ISwitchManager;
import org.opendaylight.controller.topologymanager.ITopologyManager;
import org.opendaylight.controller.topologymanager.ITopologyManagerClusterWideAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class DijkstraImplementation implements IDijkstraInterface, ITopologyManagerClusterWideAware {

    private static Logger log = LoggerFactory.getLogger(DijkstraImplementation.class);
    private ConcurrentMap<Short, Graph<Node, Edge>> topologyBWAware;
    private ConcurrentMap<Short, DijkstraShortestPath<Node, Edge>> sptBWAware;
    DijkstraShortestPath<Node, Edge> mtp; // Max Throughput Path
    private Set<IListenRoutingUpdates> routingAware;
    private Set<IListenRoutingUpdatesWrapper> myRoutingAware;
    private ISwitchManager switchManager;
    private ITopologyManager topologyManager;
    private static final long DEFAULT_LINK_SPEED = Bandwidth.BW1Gbps;
    private IClusterContainerServices clusterContainerService;

    @Override
    public void setListenRoutingUpdatesPrivate(IListenRoutingUpdatesWrapper i) {
        if (this.myRoutingAware == null) {
            this.myRoutingAware = new HashSet<>();
        }
        if (this.myRoutingAware != null) {
            log.debug("Adding routingAware listener: {}", i);
            this.myRoutingAware.add(i);
        }
    }

    public void setListenRoutingUpdates(final IListenRoutingUpdates i) {
        if (this.routingAware == null) {
            this.routingAware = new HashSet<IListenRoutingUpdates>();
        }
        if (this.routingAware != null) {
            log.debug("Adding routingAware listener: {}", i);
            this.routingAware.add(i);
        }
    }

    public void unsetListenRoutingUpdates(final IListenRoutingUpdates i) {
        if (this.routingAware == null) {
            return;
        }
        log.debug("Removing routingAware listener");
        this.routingAware.remove(i);
        if (this.routingAware.isEmpty()) {
            // We don't have any listener lets dereference
            this.routingAware = null;
        }
    }

    @Override
    public synchronized void initMaxThroughput(
            final Map<Edge, Number> EdgeWeightMap) {
        if (mtp != null) {
            log.error("Max Throughput Dijkstra is already enabled!");
            return;
        }
        Transformer<Edge, ? extends Number> mtTransformer = null;
        if (EdgeWeightMap == null) {
            mtTransformer = new Transformer<Edge, Double>() {
                @Override
                public Double transform(Edge e) {
                    if (switchManager == null) {
                        log.error("switchManager is null");
                        return (double) -1;
                    }
                    NodeConnector srcNC = e.getTailNodeConnector();
                    NodeConnector dstNC = e.getHeadNodeConnector();
                    if ((srcNC == null) || (dstNC == null)) {
                        log.error("srcNC:{} or dstNC:{} is null", srcNC, dstNC);
                        return (double) -1;
                    }
                    Bandwidth bwSrc = (Bandwidth) switchManager.getNodeConnectorProp(srcNC,
                                                                                     Bandwidth.BandwidthPropName);
                    Bandwidth bwDst = (Bandwidth) switchManager.getNodeConnectorProp(dstNC,
                                                                                     Bandwidth.BandwidthPropName);

                    long srcLinkSpeed = 0, dstLinkSpeed = 0;
                    if ((bwSrc == null) || ((srcLinkSpeed = bwSrc.getValue()) == 0)) {
                        log.debug("srcNC: {} - Setting srcLinkSpeed to Default!", srcNC);
                        srcLinkSpeed = DEFAULT_LINK_SPEED;
                    }

                    if ((bwDst == null) || ((dstLinkSpeed = bwDst.getValue()) == 0)) {
                        log.debug("dstNC: {} - Setting dstLinkSpeed to Default!", dstNC);
                        dstLinkSpeed = DEFAULT_LINK_SPEED;
                    }

                    // TODO: revisit the logic below with the real use case in
                    // mind
                    // For now we assume the throughput to be the speed of the
                    // link itself
                    // this kind of logic require information that should be
                    // polled by statistic manager and are not yet available,
                    // also this service at the moment is not used, so to be
                    // revisited later on
                    long avlSrcThruPut = srcLinkSpeed;
                    long avlDstThruPut = dstLinkSpeed;

                    // Use lower of the 2 available throughput as the available
                    // throughput
                    long avlThruPut = avlSrcThruPut < avlDstThruPut ? avlSrcThruPut : avlDstThruPut;

                    if (avlThruPut <= 0) {
                        log.debug("Edge {}: Available Throughput {} <= 0!", e, avlThruPut);
                        return (double) -1;
                    }
                    return (double) (Bandwidth.BW1Pbps / avlThruPut);
                }
            };
        } else {
            mtTransformer = new Transformer<Edge, Number>() {
                @Override
                public Number transform(Edge e) {
                    return EdgeWeightMap.get(e);
                }
            };
        }
        Short baseBW = Short.valueOf((short) 0);
        // Initialize mtp also using the default topo
        Graph<Node, Edge> g = this.topologyBWAware.get(baseBW);
        if (g == null) {
            log.error("Default Topology Graph is null");
            return;
        }
        mtp = new DijkstraShortestPath<Node, Edge>(g, mtTransformer);
    }

    @Override
    public Path getRoute(final Node src, final Node dst) {
        if ((src == null) || (dst == null)) {
            return null;
        }
        return getRoute(src, dst, (short) 0);
    }

    @Override
    public synchronized Path getRouteWithoutNode(final Node src, final Node dst, final Node middle) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public synchronized Path getRouteWithoutAllEdges(Node src, Node dst, List<Edge> edges) {
        ConcurrentMap<Short, Graph<Node, Edge>> tmpTopologyBWAware;
        ConcurrentHashMap<Short, DijkstraShortestPath<Node, Edge>> tmpSptBWAware;
        tmpTopologyBWAware = new ConcurrentHashMap<>();
        tmpSptBWAware = new ConcurrentHashMap<>();

        copyTopology(tmpTopologyBWAware, tmpSptBWAware);
        for (Edge edgeToRemove : edges) {
            edgeUpdate(edgeToRemove, UpdateType.REMOVED, null, true, tmpTopologyBWAware, tmpSptBWAware);
        }

        return getRoute(src, dst, (short) 0, tmpSptBWAware);
    }

    @Override
    public synchronized HashMap<NodeConnector, List<Path>> getRouteWithoutSingleEdges(NodeConnector srcNodeConnector, Node dst, List<Edge> edges) {
        HashMap<NodeConnector, List<Path>> routes = new HashMap<>();
        ConcurrentMap<Short, Graph<Node, Edge>> tmpTopologyBWAware;
        ConcurrentHashMap<Short, DijkstraShortestPath<Node, Edge>> tmpSptBWAware;
        tmpTopologyBWAware = new ConcurrentHashMap<>();
        tmpSptBWAware = new ConcurrentHashMap<>();

        copyTopology(tmpTopologyBWAware, tmpSptBWAware);
        for (int i = 0; i < edges.size(); i++) {
            Edge edgeToRemove = edges.get(i);
            edgeUpdate(edgeToRemove, UpdateType.REMOVED, null, true, tmpTopologyBWAware, tmpSptBWAware);
            Path route = getRoute(edgeToRemove.getTailNodeConnector().getNode(), dst, (short) 0, tmpSptBWAware);
            if (route != null) {
                if (i == 0) {
                    HashMapUtils.putMultiValue(routes, srcNodeConnector, route);
                } else {
                    HashMapUtils.putMultiValue(routes, edges.get(i - 1).getHeadNodeConnector(), route);
                }
            }
            edgeUpdate(edgeToRemove, UpdateType.ADDED, null, true, tmpTopologyBWAware, tmpSptBWAware);
        }

        return routes;
    }

    private void copyTopology(ConcurrentMap<Short, Graph<Node, Edge>> topologyBWAware, ConcurrentHashMap<Short, DijkstraShortestPath<Node, Edge>> sptBWAware) {
        Short sZero = Short.valueOf((short) 0);
        Graph<Node, Edge> g = this.topologyBWAware.get(sZero);
        //copy
        for (Edge edge : g.getEdges()) {
            updateTopo(edge, sZero, UpdateType.ADDED, topologyBWAware, sptBWAware);
        }
    }

    @Override
    public synchronized Path getRouteWithoutEdge(final Node src, final Node dst, final Edge edgeToRemove) {

        ConcurrentMap<Short, Graph<Node, Edge>> topologyBWAware;
        ConcurrentHashMap<Short, DijkstraShortestPath<Node, Edge>> sptBWAware;
        topologyBWAware = new ConcurrentHashMap<>();
        sptBWAware = new ConcurrentHashMap<>();

        copyTopology(topologyBWAware, sptBWAware);

        edgeUpdate(edgeToRemove, UpdateType.REMOVED, null, true, topologyBWAware, sptBWAware);

        return getRoute(src, dst, (short) 0, sptBWAware);
    }

    @Override
    public synchronized Path getMaxThroughputRoute(Node src, Node dst) {
        if (mtp == null) {
            log.error("Max Throughput Path Calculation Uninitialized!");
            return null;
        }

        List<Edge> path;
        try {
            path = mtp.getMaxThroughputPath(src, dst);
        } catch (IllegalArgumentException ie) {
            log.debug("A vertex is yet not known between {} {}", src, dst);
            return null;
        }
        Path res;
        try {
            res = new Path(path);
        } catch (ConstructionException e) {
            log.debug("A vertex is yet not known between {} {}", src, dst);
            return null;
        }
        return res;
    }

    public static Path getRoute(final Node src, final Node dst, final Short Bw,
                                ConcurrentMap<Short, DijkstraShortestPath<Node, Edge>> sptBWAware) {
        DijkstraShortestPath<Node, Edge> spt = sptBWAware.get(Bw);
        if (spt == null) {
            return null;
        }
        List<Edge> path;
        try {
            path = spt.getPath(src, dst);
        } catch (IllegalArgumentException ie) {
            log.debug("A vertex is yet not known between {} {}", src, dst);
            return null;
        }
        Path res;
        try {
            res = new Path(path);
        } catch (ConstructionException e) {
            log.debug("A vertex is yet not known between {} {}", src, dst);
            return null;
        }
        return res;
    }

    @Override
    public synchronized Path getRoute(final Node src, final Node dst, final Short Bw) {
        DijkstraShortestPath<Node, Edge> spt = this.sptBWAware.get(Bw);
        if (spt == null) {
            return null;
        }
        List<Edge> path;
        try {
            path = spt.getPath(src, dst);
        } catch (IllegalArgumentException ie) {
            log.debug("A vertex is yet not known between {} {}", src, dst);
            return null;
        }
        Path res;
        try {
            res = new Path(path);
        } catch (ConstructionException e) {
            log.debug("A vertex is yet not known between {} {}", src, dst);
            return null;
        }
        return res;
    }

    @Override
    public synchronized void clear() {
        DijkstraShortestPath<Node, Edge> spt;
        for (Short bw : this.sptBWAware.keySet()) {
            spt = this.sptBWAware.get(bw);
            if (spt != null) {
                spt.reset();
            }
        }
        clearMaxThroughput();
    }

    @Override
    public synchronized void clearMaxThroughput() {
        if (mtp != null) {
            mtp.reset(); // reset max throughput path
        }
    }

    private static boolean updateTopo(Edge edge,
                                      Short bw,
                                      UpdateType type,
                                      ConcurrentMap<Short, Graph<Node, Edge>> topologyBWAware,
                                      ConcurrentHashMap<Short, DijkstraShortestPath<Node, Edge>> sptBWAware) {
        Short baseBW = Short.valueOf((short) 0);
        Graph<Node, Edge> topo = topologyBWAware.get(baseBW);
        DijkstraShortestPath<Node, Edge> spt = sptBWAware.get(baseBW);
        boolean edgePresentInGraph = false;

        if (topo == null) {
            // Create topology for this BW
            Graph<Node, Edge> g = new SparseMultigraph();
            topologyBWAware.put(bw, g);
            topo = topologyBWAware.get(bw);
            sptBWAware.put(bw, new DijkstraShortestPath(g));
            spt = sptBWAware.get(bw);
        }
        if (topo != null) {
            NodeConnector src = edge.getTailNodeConnector();
            NodeConnector dst = edge.getHeadNodeConnector();
            if (spt == null) {
                spt = new DijkstraShortestPath(topo);
                sptBWAware.put(bw, spt);
            }

            switch (type) {
                case ADDED:
                    // Make sure the vertex are there before adding the edge
                    topo.addVertex(src.getNode());
                    topo.addVertex(dst.getNode());
                    // Add the link between
                    edgePresentInGraph = topo.containsEdge(edge);
                    if (edgePresentInGraph == false) {
                        try {
                            topo.addEdge(new Edge(src, dst), src.getNode(), dst.getNode(), EdgeType.DIRECTED);
                        } catch (final ConstructionException e) {
                            log.error("", e);
                            return edgePresentInGraph;
                        }
                    }
                case CHANGED:
                    // Mainly raised only on properties update, so not really useful
                    // in this case
                    break;
                case REMOVED:
                    // Remove the edge
                    try {
                        topo.removeEdge(new Edge(src, dst));
                    } catch (final ConstructionException e) {
                        log.error("", e);
                        return edgePresentInGraph;
                    }

                    // If the src and dst vertex don't have incoming or
                    // outgoing links we can get ride of them
                    if (topo.containsVertex(src.getNode()) && (topo.inDegree(src.getNode()) == 0)
                            && (topo.outDegree(src.getNode()) == 0)) {
                        log.debug("Removing vertex {}", src);
                        topo.removeVertex(src.getNode());
                    }

                    if (topo.containsVertex(dst.getNode()) && (topo.inDegree(dst.getNode()) == 0)
                            && (topo.outDegree(dst.getNode()) == 0)) {
                        log.debug("Removing vertex {}", dst);
                        topo.removeVertex(dst.getNode());
                    }
                    break;
            }
            spt.reset();
            if (bw.equals(baseBW)) {
                //TODO: for now this doesn't work
//                clearMaxThroughput();
            }
        } else {
            log.error("Cannot find topology for BW {} this is unexpected!", bw);
        }
        return edgePresentInGraph;
    }

    private static boolean edgeUpdate(Edge e,
                                      UpdateType type,
                                      Set<Property> props,
                                      boolean local,
                                      ConcurrentMap<Short, Graph<Node, Edge>> topologyBWAware,
                                      ConcurrentHashMap<Short, DijkstraShortestPath<Node, Edge>> sptBWAware) {
        String srcType = null;
        String dstType = null;

        log.trace("Got an edgeUpdate: {} props: {} update type: {} local: {}", new Object[]{e, props, type, local});

        if ((e == null) || (type == null)) {
            log.error("Edge or Update type are null!");
            return false;
        } else {
            srcType = e.getTailNodeConnector().getType();
            dstType = e.getHeadNodeConnector().getType();

            if (srcType.equals(NodeConnector.NodeConnectorIDType.PRODUCTION)) {
                log.debug("Skip updates for {}", e);
                return false;
            }

            if (dstType.equals(NodeConnector.NodeConnectorIDType.PRODUCTION)) {
                log.debug("Skip updates for {}", e);
                return false;
            }
        }

        Bandwidth bw = new Bandwidth(0);
        boolean newEdge = false;
        if (props != null) {
            props.remove(bw);
        }

        Short baseBW = Short.valueOf((short) 0);
        // Update base topo
        newEdge = !updateTopo(e, baseBW, type, topologyBWAware, sptBWAware);
        if (newEdge == true) {
            if (bw.getValue() != baseBW) {
                // Update BW topo
                updateTopo(e, (short) bw.getValue(), type, topologyBWAware, sptBWAware);
            }
        }
        return newEdge;
    }

    @SuppressWarnings({"unchecked"})
    private synchronized boolean updateTopo(Edge edge, Short bw, UpdateType type) {
        Graph<Node, Edge> topo = this.topologyBWAware.get(bw);
        DijkstraShortestPath<Node, Edge> spt = this.sptBWAware.get(bw);
        boolean edgePresentInGraph = false;
        Short baseBW = Short.valueOf((short) 0);

        if (topo == null) {
            // Create topology for this BW
            Graph<Node, Edge> g = new SparseMultigraph();
            this.topologyBWAware.put(bw, g);
            topo = this.topologyBWAware.get(bw);
            this.sptBWAware.put(bw, new DijkstraShortestPath(g));
            spt = this.sptBWAware.get(bw);
        }

        if (topo != null) {
            NodeConnector src = edge.getTailNodeConnector();
            NodeConnector dst = edge.getHeadNodeConnector();
            if (spt == null) {
                spt = new DijkstraShortestPath(topo);
                this.sptBWAware.put(bw, spt);
            }

            switch (type) {
                case ADDED:
                    // Make sure the vertex are there before adding the edge
                    topo.addVertex(src.getNode());
                    topo.addVertex(dst.getNode());
                    // Add the link between
                    edgePresentInGraph = topo.containsEdge(edge);
                    if (edgePresentInGraph == false) {
                        try {
                            topo.addEdge(new Edge(src, dst), src.getNode(), dst.getNode(), EdgeType.DIRECTED);
                        } catch (final ConstructionException e) {
                            log.error("", e);
                            return edgePresentInGraph;
                        }
                    }
                case CHANGED:
                    // Mainly raised only on properties update, so not really useful
                    // in this case
                    break;
                case REMOVED:
                    // Remove the edge
                    try {
                        topo.removeEdge(new Edge(src, dst));
                    } catch (final ConstructionException e) {
                        log.error("", e);
                        return edgePresentInGraph;
                    }

                    // If the src and dst vertex don't have incoming or
                    // outgoing links we can get ride of them
                    if (topo.containsVertex(src.getNode()) && (topo.inDegree(src.getNode()) == 0)
                            && (topo.outDegree(src.getNode()) == 0)) {
                        log.debug("Removing vertex {}", src);
                        topo.removeVertex(src.getNode());
                    }

                    if (topo.containsVertex(dst.getNode()) && (topo.inDegree(dst.getNode()) == 0)
                            && (topo.outDegree(dst.getNode()) == 0)) {
                        log.debug("Removing vertex {}", dst);
                        topo.removeVertex(dst.getNode());
                    }
                    break;
            }
            spt.reset();
            if (bw.equals(baseBW)) {
                clearMaxThroughput();
            }
        } else {
            log.error("Cannot find topology for BW {} this is unexpected!", bw);
        }
        return edgePresentInGraph;
    }

    private boolean edgeUpdate(Edge e, UpdateType type, Set<Property> props, boolean local) {
        String srcType = null;
        String dstType = null;

        log.trace("Got an edgeUpdate: {} props: {} update type: {} local: {}", new Object[]{e, props, type, local});

        if ((e == null) || (type == null)) {
            log.error("Edge or Update type are null!");
            return false;
        } else {
            srcType = e.getTailNodeConnector().getType();
            dstType = e.getHeadNodeConnector().getType();

            if (srcType.equals(NodeConnector.NodeConnectorIDType.PRODUCTION)) {
                log.debug("Skip updates for {}", e);
                return false;
            }

            if (dstType.equals(NodeConnector.NodeConnectorIDType.PRODUCTION)) {
                log.debug("Skip updates for {}", e);
                return false;
            }
        }

        Bandwidth bw = new Bandwidth(0);
        boolean newEdge = false;
        if (props != null) {
            props.remove(bw);
        }

        Short baseBW = Short.valueOf((short) 0);
        // Update base topo
        newEdge = !updateTopo(e, baseBW, type);
        if (newEdge == true) {
            if (bw.getValue() != baseBW) {
                // Update BW topo
                updateTopo(e, (short) bw.getValue(), type);
            }
        }
        return newEdge;
    }

    @Override
    public void edgeUpdate(List<TopoEdgeUpdate> topoedgeupdateList) {
        log.trace("Start of a Bulk EdgeUpdate with " + topoedgeupdateList.size() + " elements");
        boolean callListeners = false;
        List<Edge> changedEdges = new ArrayList<>();
        for (int i = 0; i < topoedgeupdateList.size(); i++) {
            Edge e = topoedgeupdateList.get(i).getEdge();
            Set<Property> p = topoedgeupdateList.get(i)
                    .getProperty();
            UpdateType type = topoedgeupdateList.get(i)
                    .getUpdateType();
            boolean isLocal = topoedgeupdateList.get(i)
                    .isLocal();
            if ((edgeUpdate(e, type, p, isLocal)) && (!callListeners)) {
                callListeners = true;
            }
            changedEdges.add(e);
        }

        // The routing listeners should only be called on the coordinator, to
        // avoid multiple controller cluster nodes to actually do the
        // recalculation when only one need to react
        boolean amICoordinator = true;
        if (this.clusterContainerService != null) {
            amICoordinator = this.clusterContainerService.amICoordinator();
        }
        if ((callListeners) && (this.routingAware != null) && amICoordinator) {
            log.trace("Calling the routing listeners");
            for (IListenRoutingUpdates ra : this.routingAware) {
                try {
                    ra.recalculateDone();
                } catch (Exception ex) {
                    log.error("Exception on routingAware listener call", ex);
                }
            }
        }
        if ((callListeners) && (this.myRoutingAware != null) && amICoordinator) {
            log.trace("Calling my routing listeners");
            for (IListenRoutingUpdatesWrapper ra : this.myRoutingAware) {
                try {
                    ra.recalculateDone(changedEdges);
                    ra.recalculateDoneTopo(topoedgeupdateList);
                } catch (Exception ex) {
                    log.error("Exception on routingAware listener call", ex);
                }
            }
        }
        log.trace("End of a Bulk EdgeUpdate");
    }

    /**
     * Function called by the dependency manager when all the required
     * dependencies are satisfied
     *
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void init() {
        log.debug("Routing init() is called");
        this.topologyBWAware = new ConcurrentHashMap<Short, Graph<Node, Edge>>();
        this.sptBWAware = new ConcurrentHashMap<Short, DijkstraShortestPath<Node, Edge>>();
        // Now create the default topology, which doesn't consider the
        // BW, also create the corresponding Dijkstra calculation
        Graph<Node, Edge> g = new SparseMultigraph();
        Short sZero = Short.valueOf((short) 0);
        this.topologyBWAware.put(sZero, g);
        this.sptBWAware.put(sZero, new DijkstraShortestPath(g));
        // Topologies for other BW will be added on a needed base
    }

    /**
     * Function called by the dependency manager when at least one dependency
     * become unsatisfied or when the component is shutting down because for
     * example bundle is being stopped.
     *
     */
    void destroy() {
        log.debug("Routing destroy() is called");
    }

    /**
     * Function called by dependency manager after "init ()" is called and after
     * the services provided by the class are registered in the service registry
     *
     */
    void start() {
        log.debug("Routing start() is called");
        // build the routing database from the topology if it exists.
        Map<Edge, Set<Property>> edges = topologyManager.getEdges();
        if (edges.isEmpty()) {
            return;
        }
        List<TopoEdgeUpdate> topoedgeupdateList = new ArrayList<TopoEdgeUpdate>();
        log.debug("Creating routing database from the topology");
        for (Iterator<Map.Entry<Edge, Set<Property>>> i = edges.entrySet()
                .iterator(); i.hasNext();) {
            Map.Entry<Edge, Set<Property>> entry = i.next();
            Edge e = entry.getKey();
            Set<Property> props = entry.getValue();
            TopoEdgeUpdate topoedgeupdate = new TopoEdgeUpdate(e, props,
                                                               UpdateType.ADDED);
            topoedgeupdateList.add(topoedgeupdate);
        }
        edgeUpdate(topoedgeupdateList);
    }

    /**
     * Function called by the dependency manager before the services exported by
     * the component are unregistered, this will be followed by a "destroy ()"
     * calls
     *
     */
    public void stop() {
        log.debug("Routing stop() is called");
    }

    public void setSwitchManager(ISwitchManager switchManager) {
        this.switchManager = switchManager;
    }

    public void unsetSwitchManager(ISwitchManager switchManager) {
        if (this.switchManager == switchManager) {
            this.switchManager = null;
        }
    }

    public void setTopologyManager(ITopologyManager tm) {
        this.topologyManager = tm;
    }

    public void unsetTopologyManager(ITopologyManager tm) {
        if (this.topologyManager == tm) {
            this.topologyManager = null;
        }
    }

    void setClusterContainerService(IClusterContainerServices s) {
        log.debug("Cluster Service set");
        this.clusterContainerService = s;
    }

    void unsetClusterContainerService(IClusterContainerServices s) {
        if (this.clusterContainerService == s) {
            log.debug("Cluster Service removed!");
            this.clusterContainerService = null;
        }
    }

    @Override
    public void edgeOverUtilized(Edge edge) {
        // TODO Auto-generated method stub

    }

    @Override
    public void edgeUtilBackToNormal(Edge edge) {
        // TODO Auto-generated method stub

    }
}
