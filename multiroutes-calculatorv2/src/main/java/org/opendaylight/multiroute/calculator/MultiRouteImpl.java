package org.opendaylight.multiroute.calculator;

import io.netty.util.internal.ConcurrentSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.opendaylight.controller.hosttracker.IfIptoHost;
import org.opendaylight.controller.hosttracker.hostAware.HostNodeConnector;
import org.opendaylight.controller.md.sal.common.api.TransactionStatus;
import org.opendaylight.controller.sal.binding.api.NotificationService;
import org.opendaylight.controller.sal.binding.api.data.DataBrokerService;
import org.opendaylight.controller.sal.core.ConstructionException;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.core.Path;
import org.opendaylight.controller.sal.core.UpdateType;
import org.opendaylight.controller.sal.routing.IDijkstraInterface;
import org.opendaylight.controller.sal.routing.IListenRoutingUpdatesWrapper;
import org.opendaylight.controller.sal.topology.TopoEdgeUpdate;
import org.opendaylight.controller.sal.utils.NetUtils;
import org.opendaylight.multiroute.cache.EthFlowsFromPathResult;
import org.opendaylight.multiroute.cache.SrcDstPair;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.FlowAdded;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.FlowRemoved;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.FlowUpdated;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.NodeErrorNotification;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.NodeExperimenterErrorNotification;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.SalFlowListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.SwitchFlowRemoved;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.FlowModFlags;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiRouteImpl implements PacketProcessingListener, SalFlowListener, IListenRoutingUpdatesWrapper {

    private final static int flow_timeout = 30;

    private static final byte[] ETH_TYPE_IPV4 = new byte[]{0x08, 0x00};
//    private static final byte[] ETH_TYPE_ARP = new byte[]{0x08, 0x06};

    /**
     * The logger instance to debug or sent some info about MultiRouteImpl.
     */
    protected static final Logger logger = LoggerFactory.getLogger(MultiRouteImpl.class);

    /**
     * IfIptoHost's instance to find hosts in the topology.
     */
    private IfIptoHost hostFinder = null;

    /**
     * IDijkstraInterface's instance to find a route between the nodes that are
     * connecting the source host and destination node, respectively.
     */
    private IDijkstraInterface dijkstraInterface = null;

    /**
     * DataBrokerService's instance to send configuration flows to the nodes.
     */
    private DataBrokerService dataBrokerService;

    /**
     * Packet processing service to send packets anywhere in the topology.
     */
    private PacketProcessingService packetProcessingService;

    /**
     * The notification service to register classes that are listeners.
     */
    private NotificationService notificationService;

    /**
     * A helper class to send flow configurations to the nodes.
     */
    private SendUtils sendU;

    private final Cache ca = Cache.getCache();
    /**
     * A queue that contains the source and destination mac addresses that are
     * concurrently being searched. This is a way to prevent multiple
     * calculations for the same src<->dst mac address and multiple flow
     * configurations. TODO: This method should be replaced by other ASAP.
     */
    private ConcurrentHashMap<Integer, Semaphore> dstNotificationThread;

    public MultiRouteImpl() {
        this.hnc = new ConcurrentSet<HostNodeConnector>();
    }

    @Override
    public void onPacketReceived(PacketReceived notification) {

        byte[] etherType = PacketUtils.extractEtherType(notification.getPayload());

        if (!Arrays.equals(ETH_TYPE_IPV4, etherType)) {
            return;
        }

        byte[] dstMacRaw = PacketUtils.extractDstMac(notification.getPayload());
        byte[] srcMacRaw = PacketUtils.extractSrcMac(notification.getPayload());

        /**
         * Ignore broadcast/multicast destination.
         */
        if (NetUtils.isBroadcastMACAddr(dstMacRaw) || NetUtils.isMulticastMACAddr(dstMacRaw)) {
            return;
        }

        MacAddress dstMac = PacketUtils.rawMacToMac(dstMacRaw);
        MacAddress srcMac = PacketUtils.rawMacToMac(srcMacRaw);

        NodeConnector srcNodeConnector;
        try {
            srcNodeConnector = InstanceIdentifierUtils.getNodeConnector(notification.getIngress().getValue());
        } catch (ConstructionException ex) {
            logger.error("ConstructionException while creating the source node connector: {}", ex.getMessage());
            return;
        }
        Node sourceNode = srcNodeConnector.getNode();
        /**
         * First we check if there is any other thread calculating this path. If
         * there is any other thread then we wait until it's calculated.
         */
        int nodeSrcDstMacHash = calculateHashCode(sourceNode, srcMac, dstMac);

        dstNotificationThread.putIfAbsent(nodeSrcDstMacHash, new Semaphore(1, true));

        try {
            if (dstNotificationThread.get(nodeSrcDstMacHash).tryAcquire(1, TimeUnit.SECONDS)) {
//        dstNotificationThread.get(nodeSrcDstMacHash).acquireUninterruptibly();
                try {
                    logger.trace("Finding destination host {} in the ingress {} ... with buffer Id {}", dstMac, notification.getIngress(), notification.getBufferId().toString());

                    HostNodeConnector destinationHost = getHostNodeConnectorFromMacAddress(dstMacRaw);

                    if (destinationHost == null) {
                        logger.warn("HostNodeConnector for {} not found", dstMac.toString());
//                dstNotificationThread.get(nodeSrcDstMacHash).readLock().unlock();
                        return;
                    } else {
                        logger.debug("HostNodeConnector found {}", destinationHost.toString());
                    }

                    Node destinationNode = destinationHost.getnodeconnectorNode();
                    NodeConnector dstNodeConnector = destinationHost.getnodeConnector();

                    logger.trace("Checking if the combination of {} src<->dst {} is already being processed on Node {}", srcMac, dstMac, sourceNode.getNodeIDString());

                    EthFlowsFromPathResult effpr = ca.getEthFlowsFromPath(srcNodeConnector, dstNodeConnector, srcMac, dstMac);
                    if (effpr.getlFbs() != null && !effpr.getlFbs().isEmpty()) {
                        logger.trace("It seems like their flow is already created let's resend their primary flow.");

                        for (FlowBuilder fb : effpr.getlFbs()) {
                            if (fb.getMatch().getInPort().getValue().equals(srcNodeConnector.getID())) {
                                fb.setBufferId(notification.getBufferId().getValue());
                                if (effpr.getSrcNodeConnector().equals(srcNodeConnector)) {
                                    fb.setFlags(new FlowModFlags(false, false, false, false, true));
                                    fb.setIdleTimeout(flow_timeout);
                                }
                            }
                        }
                        List<Future<RpcResult<TransactionStatus>>> sendFlowsToNode = sendU.resendFlowsToNode(sourceNode, effpr.getlFbs());
                        for (Future<RpcResult<TransactionStatus>> f : sendFlowsToNode) {
                            try {
                                f.get();
                            } catch (InterruptedException | ExecutionException ex) {
                            }
                        }
//                dstNotificationThread.get(nodeSrcDstMacHash).readLock().unlock();
                        return;
                    }

                    /**
                     * Now we have: the sourceNode - the node that directly
                     * connects the source host; the destinationNode - the node
                     * that directly connects the destination host; the
                     * srcNodeConnector - the node connector that is directly
                     * connected to the source host; the dstNodeConnector - the
                     * node connector that is directly connected to the
                     * destination host; We can now start searching the primary
                     * and backup routes.
                     */
                    logger.debug("Finding routes for {} to {}", sourceNode.toString(), destinationNode.toString());
                    Path primaryRoute = dijkstraInterface.getRoute(sourceNode, destinationNode);
                    List<Future<RpcResult<TransactionStatus>>> sendFlowsForBackupRoute = new ArrayList<>();
                    boolean configureRoundTrip = false;
                    if (primaryRoute != null) {

                        logger.trace("primaryRoute route {}", primaryRoute.toString());

                        HashMap<NodeConnector, List<Path>> routeWithoutSingleEdges = dijkstraInterface.getRouteWithoutSingleEdges(srcNodeConnector, destinationNode, primaryRoute.getEdges());

                        sendFlowsForBackupRoute.addAll(sendU.programNodes(srcNodeConnector,
                                                                          dstNodeConnector,
                                                                          srcMac,
                                                                          dstMac,
                                                                          primaryRoute,
                                                                          routeWithoutSingleEdges,
                                                                          notification,
                                                                          configureRoundTrip));
                        for (Future<RpcResult<TransactionStatus>> f : sendFlowsForBackupRoute) {
                            try {
                                f.get();
                            } catch (InterruptedException | ExecutionException ex) {
                            }
                        }

                    } else {
                        logger.trace("No primary route found");
                        if (srcNodeConnector.getNode().equals(dstNodeConnector.getNode())) {
                            sendU.programNode(srcNodeConnector,
                                              dstNodeConnector,
                                              srcMac,
                                              dstMac,
                                              notification);
                        }
                    }

                    logger.debug("Finish for {} -> {}", srcMac.toString(), dstMac.toString());
                } finally {
                    dstNotificationThread.get(nodeSrcDstMacHash).release();
                }
            }
        } catch (InterruptedException ex) {
        }
    }

    @Override
    public synchronized void recalculateDone(List<Edge> changeEdges) {

    }

    @Override
    public void recalculateDone() {

    }

    @Override
    public void onFlowAdded(FlowAdded notification) {
    }

    @Override
    public void onFlowRemoved(FlowRemoved notification) {
    }

    @Override
    public void onFlowUpdated(FlowUpdated notification) {
    }

    @Override
    public void onNodeErrorNotification(NodeErrorNotification notification) {
    }

    @Override
    public void onNodeExperimenterErrorNotification(NodeExperimenterErrorNotification notification) {
    }

    @Override
    public void onSwitchFlowRemoved(SwitchFlowRemoved notification) {

        logger.debug("Entering onSwitchFlowRemoved");
        if (notification.getRemovedReason().isIDLETIMEOUT()) {
            long cookieId = notification.getCookie().longValue();
            SrcDstPair sdp = ca.getSrcDstPair(cookieId);
            if (sdp == null) {
                return;
            }
            int nodeSrcDstMacHash = calculateHashCode(sdp.getSrcNodeConnector().getNode(), sdp.getSrcEth(), sdp.getDstEth());
//        dstNotificationThread.putIfAbsent(nodeSrcDstMacHash, new Semaphore(1, true));
            dstNotificationThread.putIfAbsent(nodeSrcDstMacHash, new Semaphore(1, true));
            dstNotificationThread.get(nodeSrcDstMacHash).acquireUninterruptibly();
            try {
                if (sdp != null) {
                    logger.debug("Found, removing cookie: {}, {}", notification.getCookie().toString(), sdp);
                    HashSet<Long> flowsIdToDel = ca.getAllFlowIdsFromSrcDstPair(sdp);
                    ca.removeCookies(flowsIdToDel);
                    ca.removePath(sdp);
                    ca.removeSrcPairId(sdp);
//            HashMap<Node, List<RemoveFlowInputBuilder>> flowsToDel = FlowUtils.createRemovableFlows(flowsIdToDel);
                    sendU.sendFlowsModToNodes(flowsIdToDel);
                    logger.debug("Flows of {} removed successfuly", sdp);
                }
            } finally {
                dstNotificationThread.get(nodeSrcDstMacHash).release();
            }
        } else {
            logger.debug("Removing a flow from cache");
            long cookieId = notification.getCookie().longValue();
            ca.removeInstanceIdentifier(cookieId);
        }
        logger.debug("Exiting onSwitchFlowRemoved");
    }

    /**
     * Set a iDijkstra service interface.
     *
     * @param iDijkstra the IDijkstraInterface's instance to be set.
     * @throws IllegalArgumentException if iDijkstra is null.
     */
    public void setDijkstraService(IDijkstraInterface iDijkstra) throws IllegalArgumentException {
        if (iDijkstra == null) {
            logger.error("IDijkstraInterface is mandatory and wasn't found any provider of it.");
            throw new IllegalArgumentException("IDijkstraInterface is mandatory so it shouldn't be null.");
        }
        logger.debug("RoutingService set");
        this.dijkstraInterface = iDijkstra;
    }

    /**
     * Set a dataBrokerService service interface.
     *
     * @param dataBrokerService the DataBrokerService's instance to be set.
     * @throws IllegalArgumentException if dataBrokerService is null.
     */
    public void setDataBrokerService(DataBrokerService dataBrokerService) throws IllegalArgumentException {
        if (dataBrokerService == null) {
            logger.error("DataBrokerService is mandatory and wasn't found any provider of it.");
            throw new IllegalArgumentException("DataBrokerService is mandatory so it shouldn't be null.");
        }
        logger.debug("DataBrokerService set");
        this.dataBrokerService = dataBrokerService;
    }

    /**
     * Set a packetProcessingService service interface.
     *
     * @param packetProcessingService the PacketProcessingService's instance to
     * be set.
     * @throws IllegalArgumentException if packetProcessingService is null.
     */
    public void setPacketProcessingService(PacketProcessingService packetProcessingService) throws IllegalArgumentException {
        if (packetProcessingService == null) {
            logger.error("PacketProcessingService is mandatory and wasn't found any provider of it.");
            throw new IllegalArgumentException("PacketProcessingService is mandatory so it shouldn't be null.");
        }
        logger.debug("PacketProcessingService set");
        this.packetProcessingService = packetProcessingService;
    }

    /**
     * Set a notificationService service interface.
     *
     * @param notificationService the NotificationService's instance to be set.
     * @throws IllegalArgumentException if notificationService is null.
     */
    public void setNotificationService(NotificationService notificationService) throws IllegalArgumentException {
        if (notificationService == null) {
            logger.error("NotificationService is mandatory and wasn't found any provider of it.");
            throw new IllegalArgumentException("NotificationService is mandatory so it shouldn't be null.");
        }
        logger.debug("NotificationService set");
        this.notificationService = notificationService;
    }

    /**
     * Set a host finder interface.
     *
     * @param hostFinder the IfIptoHost's instance to be set.
     * @throws IllegalArgumentException if hostFinder is null.
     */
    public void setHostFinder(IfIptoHost hostFinder) throws IllegalArgumentException {
        if (hostFinder == null) {
            logger.error("IfIptoHost is mandatory and wasn't found any provider of it.");
            throw new IllegalArgumentException("IfIptoHost is mandatory so it shouldn't be null.");
        }
        logger.debug("HostFinder set");
        this.hostFinder = hostFinder;
    }

    /**
     * Starts the multiRouteImpl internal services.
     */
    public void start() {
        logger.info("Start.");
        notificationService.registerNotificationListener(this);
        sendU = new SendUtils();
        sendU.setPacketProcessingService(packetProcessingService);
        sendU.setDataBrokerService(dataBrokerService);
        dstNotificationThread = new ConcurrentHashMap<>();
        dijkstraInterface.setListenRoutingUpdatesPrivate(this);
    }

    public void stop() {
        logger.info("Stop.");
    }
    private final ConcurrentSet<HostNodeConnector> hnc;

    @Override
    public void recalculateDoneTopo(List<TopoEdgeUpdate> listTeu) {
        logger.debug("Entering recalculateDone");
        boolean removed = false;
        final List<Edge> changeEdges = new ArrayList<>();
        for (TopoEdgeUpdate teu : listTeu) {
            if (teu.getUpdateType().equals(UpdateType.REMOVED)) {
                removed = true;
            }
            changeEdges.add(teu.getEdge());
        }
        final HashSet<SrcDstPair> srcDstPairsDisrupted;
        if (removed) {
            srcDstPairsDisrupted = ca.getSrcDstPairFrom(changeEdges);
        } else {
            Collection<SrcDstPair> allSrcDstPair = ca.getAllSrcDstPair();
            srcDstPairsDisrupted = new HashSet<>(allSrcDstPair);
        }
        Runnable r = new Runnable() {

            @Override
            public void run() {
                /**
                 * When I was developing this bundle I thought it was a good
                 * idea only to get the routes for the changed edges. It turns
                 * out that it was dangerous if a new backup path is found but
                 * isn't saved on cache.
                 */

                Iterator<SrcDstPair> iterator = srcDstPairsDisrupted.iterator();
                while (iterator.hasNext()) {
                    final SrcDstPair next = iterator.next();
                    Runnable t = new Runnable() {
                        @Override
                        public void run() {
                            int nodeSrcDstMacHash = calculateHashCode(next.getSrcNodeConnector().getNode(), next.getSrcEth(), next.getDstEth());
                            dstNotificationThread.putIfAbsent(nodeSrcDstMacHash, new Semaphore(1, true));
                            dstNotificationThread.get(nodeSrcDstMacHash).acquireUninterruptibly();
                            try {
                                Path primaryRoute = dijkstraInterface.getRoute(next.getSrcNodeConnector().getNode(), next.getDstNodeConnector().getNode());
                                if (primaryRoute != null) {
                                    logger.debug("Reconfiguring a new route for {}", next);
                                    HashMap<NodeConnector, List<Path>> routeWithoutSingleEdges = dijkstraInterface.getRouteWithoutSingleEdges(next.getSrcNodeConnector(), next.getDstNodeConnector().getNode(), primaryRoute.getEdges());
                                    ca.removeEdgesForSrcPairId(next);
                                    Set<Map.Entry<NodeConnector, List<Path>>> entrySet = routeWithoutSingleEdges.entrySet();
                                    for (Map.Entry<NodeConnector, List<Path>> e : entrySet) {
                                        for (Path p : e.getValue()) {
                                            logger.trace("Node {} Secondary route {}", e.getKey(), p);
                                        }
                                    }
                                    List<Future<RpcResult<TransactionStatus>>> sendFlowsModToNodes = sendU.programNodes(next.getSrcNodeConnector(),
                                                                                                                        next.getDstNodeConnector(),
                                                                                                                        next.getSrcEth(),
                                                                                                                        next.getDstEth(),
                                                                                                                        primaryRoute,
                                                                                                                        routeWithoutSingleEdges,
                                                                                                                        null,
                                                                                                                        false);
                                    for (Future<RpcResult<TransactionStatus>> f : sendFlowsModToNodes) {
                                        try {
                                            f.get();
                                        } catch (InterruptedException | ExecutionException ex) {
                                        }
                                    }
                                } else {
                                    logger.trace("No primary route found");
                                    List<Future<RpcResult<TransactionStatus>>> sendFlowsModToNodes = new ArrayList<>();
                                    if (next.getSrcNodeConnector().getNode().equals(next.getDstNodeConnector().getNode())) {
                                        sendFlowsModToNodes.add(sendU.programNode(next.getSrcNodeConnector(),
                                                                                  next.getDstNodeConnector(),
                                                                                  next.getSrcEth(),
                                                                                  next.getDstEth(),
                                                                                  null));
                                    } else {
                                        HashSet<Long> flowsIdToDel = ca.getAllFlowIdsFromSrcDstPair(next);
                                        ca.removeCookies(flowsIdToDel);
                                        ca.removePath(next);
                                        ca.removeSrcPairId(next);
                                        sendFlowsModToNodes.addAll(sendU.sendFlowsModToNodes(flowsIdToDel));
                                        logger.debug("Flows of {} removed successfuly", next);
                                    }
                                    for (Future<RpcResult<TransactionStatus>> f : sendFlowsModToNodes) {
                                        try {
                                            f.get();
                                        } catch (InterruptedException | ExecutionException ex) {
                                        }
                                    }
                                }
                            } finally {
                                for (Edge e : changeEdges) {
                                    logger.debug("Reconfigured link for {}", e);
                                }
                                dstNotificationThread.get(nodeSrcDstMacHash).release();
                            }
                        }
                    };
                    Thread t1 = new Thread(t);
                    t1.start();
                }
            }
        };
        Thread t = new Thread(r);
        t.start();
        logger.debug("Exiting recalculateDone");
    }

    /**
     * Searches and returns the first HostNodeConnector that contains the giving
     * macRaw address.
     *
     * @param hnc Set containing all HostNodeConnector to search.
     * @param macRaw The Mac address to search for.
     * @return The respective HostNodeConnector that contains the given macRaw,
     * null if wasn't found.
     */
    private HostNodeConnector getHostNodeConnectorFromMacAddress(byte[] macRaw) {
        HostNodeConnector foundHost = null;
        synchronized (hnc) {
            for (HostNodeConnector nc : hnc) {
                if (Arrays.equals(nc.getDataLayerAddressBytes(), macRaw)) {
                    foundHost = nc;
                    break;
                }
            }
        }
        if (foundHost == null) {
            synchronized (hnc) {
                hnc.clear();
                hnc.addAll(hostFinder.getAllHosts());
                for (HostNodeConnector nc : hnc) {
                    if (Arrays.equals(nc.getDataLayerAddressBytes(), macRaw)) {
                        foundHost = nc;
                        break;
                    }
                }
            }
        }
        return foundHost;
    }

    /**
     * Method to be called when it's to finish the thread. The method will be
     * wait until every transaction is finish.
     *
     * @param arrayTransactionStatus List with all the transaction status to be
     * waited.
     * @param keys The list of keys to be removed from the dstNotificationThread
     * list.
     */
    private void waitAndRelease(List<Future<RpcResult<TransactionStatus>>> arrayTransactionStatus, int keys) {

        logger.debug("Waiting for every transaction status");
        for (Future<RpcResult<TransactionStatus>> f : arrayTransactionStatus) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException ex) {
            }
        }
        logger.debug("Every transaction sent");
//        dstNotificationThread.get(keys).release();
    }

    private static int calculateHashCode(Node sourceNode, MacAddress srcMac, MacAddress dstMac) {
        final int prime = 31;
        int nodeSrcDstMacHash = 1;
        nodeSrcDstMacHash = prime * nodeSrcDstMacHash + sourceNode.hashCode();
        nodeSrcDstMacHash = prime * nodeSrcDstMacHash + ((srcMac == null) ? 0 : srcMac.hashCode());
        nodeSrcDstMacHash = prime * nodeSrcDstMacHash + ((dstMac == null) ? 0 : dstMac.hashCode());
        return nodeSrcDstMacHash;
    }

}
