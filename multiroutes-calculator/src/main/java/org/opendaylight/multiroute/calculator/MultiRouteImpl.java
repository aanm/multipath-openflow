package org.opendaylight.multiroute.calculator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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
import org.opendaylight.controller.sal.routing.IDijkstraInterface;
import org.opendaylight.controller.sal.routing.IListenRoutingUpdatesWrapper;
import org.opendaylight.controller.sal.topology.TopoEdgeUpdate;
import org.opendaylight.controller.sal.utils.NetUtils;
import org.opendaylight.multiroute.cache.CachedRoute;
import org.opendaylight.multiroute.cache.Route;
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
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiRouteImpl implements PacketProcessingListener, SalFlowListener, IListenRoutingUpdatesWrapper {

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

        logger.trace("Checking if the combination of {} src<->dst {} is already being processed on Node {}", srcMac, dstMac, sourceNode.getNodeIDString());
        /**
         * First we check if there is any other thread calculating this path. If
         * there is any other thread then we wait until it's calculated.
         */
        final int prime = 31;
        int nodeSrcDstMacHash = 1;
        nodeSrcDstMacHash = prime * nodeSrcDstMacHash + sourceNode.hashCode();
        nodeSrcDstMacHash = prime * nodeSrcDstMacHash + ((srcMac == null) ? 0 : srcMac.hashCode());
        nodeSrcDstMacHash = prime * nodeSrcDstMacHash + ((dstMac == null) ? 0 : dstMac.hashCode());
        dstNotificationThread.putIfAbsent(nodeSrcDstMacHash, new Semaphore(1, true));

        if (dstNotificationThread.get(nodeSrcDstMacHash).tryAcquire()) {
            FlowBuilder fb = ca.getFlowBuilder(sourceNode, srcMac, dstMac);
            if (fb != null) {
                logger.trace("It seems like their flow is already created let's resend their primary flow.");
                /**
                 * It also means that the database has it's primary path.
                 */
                CachedRoute cr = ca.getRoute(sourceNode, srcMac, dstMac);
                /**
                 * If cr is null it means that the node asking for directions is
                 * one of the core nodes.
                 */

                List<Future<RpcResult<TransactionStatus>>> sendFlowsForBackupRoute = new ArrayList<>();
                if (cr != null) {
                    sendFlowsForBackupRoute.addAll(sendU.sendFlowsForBackupRoute(cr.getSrcNodeCon(),
                                                                                 cr.getDstNodeCon(),
                                                                                 srcMac,
                                                                                 dstMac,
                                                                                 cr.getBackup(),
                                                                                 true));

                    sendFlowsForBackupRoute.addAll(sendU.sendFlowsForRoute(cr.getSrcNode(),
                                                                           cr.getSrcNodeCon(),
                                                                           cr.getDstNode(),
                                                                           cr.getDstNodeCon(),
                                                                           srcMac,
                                                                           dstMac,
                                                                           cr.getPrincipal(),
                                                                           cr.getBackup(),
                                                                           notification,
                                                                           true));
                } else {
                    fb.setBufferId(notification.getBufferId().getValue());
                    sendFlowsForBackupRoute.add(sendU.writeFlowToNode(sourceNode, fb.build()));
                }
//                waitAndRelease(sendFlowsForBackupRoute, nodeSrcDstMacHash);
                dstNotificationThread.get(nodeSrcDstMacHash).release();
                return;
            }
        } else {
            try {
                if (!dstNotificationThread.get(nodeSrcDstMacHash).tryAcquire(1, TimeUnit.SECONDS)) {
                    return;
                }
            } catch (InterruptedException ex) {
            }
            FlowBuilder fb = ca.getFlowBuilder(sourceNode, srcMac, dstMac);
            if (fb != null) {
                logger.trace("It seems like their flow is already created let's resend their primary flow.");
                /**
                 * It also means that the database has it's path.
                 */
                fb.setBufferId(notification.getBufferId().getValue());
                sendU.writeFlowToNode(sourceNode, fb.build());
                dstNotificationThread.get(nodeSrcDstMacHash).release();
                return;
            }
        }

        logger.trace("Finding destination host {} in the ingress {} ... with buffer Id {}", dstMac, notification.getIngress(), notification.getBufferId().toString());
        Set<HostNodeConnector> hosts = hostFinder.getAllHosts();
        HostNodeConnector destinationHost = getHostNodeConnectorFromMacAddress(hosts, dstMacRaw);

        if (destinationHost == null) {
            logger.warn("HostNodeConnector for {} not found", dstMac.toString());
            dstNotificationThread.get(nodeSrcDstMacHash).release();
            return;
        } else {
            logger.debug("HostNodeConnector found {}", destinationHost.toString());
        }

        Node destinationNode = destinationHost.getnodeconnectorNode();
        NodeConnector dstNodeConnector = destinationHost.getnodeConnector();

        /**
         * Now we have: the sourceNode - the node that directly connects the
         * source host; the destinationNode - the node that directly connects
         * the destination host; the srcNodeConnector - the node connector that
         * is directly connected to the source host; the dstNodeConnector - the
         * node connector that is directly connected to the destination host; We
         * can now start searching the primary and backup routes.
         */
        logger.debug("Finding routes for {} to {}", sourceNode.toString(), destinationNode.toString());
        Path primaryRoute = dijkstraInterface.getRoute(sourceNode, destinationNode);
        Path backupRoute;
        List<Future<RpcResult<TransactionStatus>>> sendFlowsForBackupRoute = new ArrayList<>();
        boolean configureRoundTrip = true;
        if (primaryRoute != null) {

            logger.trace("primaryRoute route {}", primaryRoute.toString());

            backupRoute = dijkstraInterface.getRouteWithoutAllEdges(sourceNode, destinationNode, primaryRoute.getEdges());

            /**
             * If the backup route is null we can't do miracles so there aren't
             * any backup paths for this flow.
             */
            if (backupRoute != null) {
                logger.trace("backUp route {}", backupRoute.toString());

                ca.insertEdgesPath(backupRoute);
                ca.insertEdgesPath(backupRoute.reverse());

            } else {
                logger.trace("No backup route found");
            }

            ca.insertEdgesPath(primaryRoute);
            ca.insertEdgesPath(primaryRoute.reverse());

            int routeIdSrcDst = ca.insertRoute(sourceNode, destinationNode, primaryRoute, backupRoute);
            ca.savePairOrig(srcMac, dstMac, srcNodeConnector, dstNodeConnector, routeIdSrcDst);

            if (configureRoundTrip) {
                int routeIdDstSrc;
                if (backupRoute == null) {
                    routeIdDstSrc = ca.insertRoute(destinationNode, sourceNode, primaryRoute.reverse(), null);
                } else {
                    routeIdDstSrc = ca.insertRoute(destinationNode, sourceNode, primaryRoute.reverse(), backupRoute.reverse());
                }
                ca.savePairOrig(dstMac, srcMac, dstNodeConnector, srcNodeConnector, routeIdDstSrc);
            }

            sendFlowsForBackupRoute.addAll(sendU.sendFlowsForBackupRoute(srcNodeConnector,
                                                                         dstNodeConnector,
                                                                         srcMac,
                                                                         dstMac,
                                                                         backupRoute,
                                                                         configureRoundTrip));

            sendFlowsForBackupRoute.addAll(sendU.sendFlowsForRoute(sourceNode,
                                                                   srcNodeConnector,
                                                                   destinationNode,
                                                                   dstNodeConnector,
                                                                   srcMac,
                                                                   dstMac,
                                                                   primaryRoute,
                                                                   backupRoute,
                                                                   notification,
                                                                   configureRoundTrip));

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
        waitAndRelease(sendFlowsForBackupRoute, nodeSrcDstMacHash);
    }

    @Override
    public void recalculateDoneTopo(List<TopoEdgeUpdate> changeEdges) {
    }

    private void recalculateRoutes(List<Route> routes) {
        logger.debug("Entering recalculateDone");
        int prime = 31;
        List<Future<RpcResult<TransactionStatus>>> sendFlowsForBackupRoute = new ArrayList<>();
        for (Route r : routes) {
            logger.debug("Reconfiguring {}", r);
            Path p = r.getPrincipal();
            Path b = r.getBackup();
            Node srcNode = r.getSrcNode();
            Node dstNode = r.getDstNode();
            Path newP = dijkstraInterface.getRoute(srcNode, dstNode);
            if (newP != null) {
                Path backupPath
                        = dijkstraInterface.getRouteWithoutAllEdges(srcNode, dstNode, newP.getEdges());

                if (p.equals(backupPath)) {
                    Path temp = newP;
                    newP = backupPath;
                    backupPath = temp;
                }

                if (!p.equals(newP) || (backupPath != null && !backupPath.equals(b))) {
                    List<SrcDstPair> sdps = ca.getSrcDstPairs(r);
                    for (SrcDstPair sdp : sdps) {
                        int nodeSrcDstMacHash = 1;
                        nodeSrcDstMacHash = prime * nodeSrcDstMacHash + sdp.getSrcNodeConnector().getNode().hashCode();
                        nodeSrcDstMacHash = prime * nodeSrcDstMacHash + ((sdp.getSrcEth() == null) ? 0 : sdp.getSrcEth().hashCode());
                        nodeSrcDstMacHash = prime * nodeSrcDstMacHash + ((sdp.getDstEth() == null) ? 0 : sdp.getDstEth().hashCode());
                        dstNotificationThread.putIfAbsent(nodeSrcDstMacHash, new Semaphore(1, true));
                        try {
                            dstNotificationThread.get(nodeSrcDstMacHash).acquire();
                        } catch (InterruptedException ex) {
                        }
                    }

                    ca.saveNewPaths(r, newP, backupPath);
//                    Path pPathReverse = newP.reverse();
//                    Path bPathReverse = backupPath == null ? null : backupPath.reverse();
//                    ca.saveNewPaths(r.reverse(), pPathReverse, bPathReverse);

                    for (SrcDstPair sdp : sdps) {
                        sendFlowsForBackupRoute.addAll(
                                sendU.configureCoreNodes(sdp.getSrcEth(),
                                                         sdp.getSrcNodeConnector(),
                                                         sdp.getDstEth(),
                                                         sdp.getDstNodeConnector(),
                                                         newP,
                                                         backupPath,
                                                         false));
                    }
                    for (SrcDstPair sdp : sdps) {
                        sendFlowsForBackupRoute.addAll(
                                sendU.configureBackUpNodes(sdp.getSrcEth(),
                                                           sdp.getDstEth(),
                                                           sdp.getDstNodeConnector(),
                                                           backupPath,
                                                           false));
                    }
                    for (SrcDstPair sdp : sdps) {
                        sendFlowsForBackupRoute.addAll(
                                sendU.configurePeripheralNodes(sdp.getSrcEth(),
                                                               sdp.getSrcNodeConnector(),
                                                               sdp.getDstEth(),
                                                               sdp.getDstNodeConnector(),
                                                               newP,
                                                               backupPath,
                                                               false));
                    }
                    for (SrcDstPair sdp : sdps) {
                        int nodeSrcDstMacHash = 1;
                        nodeSrcDstMacHash = prime * nodeSrcDstMacHash + sdp.getSrcNodeConnector().getNode().hashCode();
                        nodeSrcDstMacHash = prime * nodeSrcDstMacHash + ((sdp.getSrcEth() == null) ? 0 : sdp.getSrcEth().hashCode());
                        nodeSrcDstMacHash = prime * nodeSrcDstMacHash + ((sdp.getDstEth() == null) ? 0 : sdp.getDstEth().hashCode());
                        dstNotificationThread.get(nodeSrcDstMacHash).release();
                    }
                }
            }
        }
        for (Future<RpcResult<TransactionStatus>> f : sendFlowsForBackupRoute) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException ex) {
            }
        }
        logger.debug("Exiting recalculateDone");
    }

    @Override
    public synchronized void recalculateDone(List<Edge> changeEdges) {
        logger.debug("recalculateDone");
        /**
         * When I was developing this bundle I thought it was a good idea only
         * to get the routes for the changed edges. It turns out that it was
         * dangerous if a new backup path is found but isn't saved on cache.
         */
        List<Route> routesDirectlyModified = ca.getAllRoutesFromEdges(changeEdges);
        List<Route> routesThatCouldGainAdvantage = ca.getAllRoutes();
        List<Route> routesToRemove = new ArrayList<>(routesDirectlyModified);
        for (Route r : routesDirectlyModified) {
            Route reverse = r.reverse();
            if (reverse != null) {
                routesToRemove.add(reverse);
            }
        }
        routesThatCouldGainAdvantage.removeAll(routesToRemove);
        logger.debug("routesDirectlyModified");
        recalculateRoutes(routesDirectlyModified);
        logger.debug("routesThatCouldGainAdvantage");
        recalculateRoutes(routesThatCouldGainAdvantage);
        for (Edge e : changeEdges) {
            logger.debug("Reconfigured link for {}", e);
        }
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

    /**
     * Searches and returns the first HostNodeConnector that contains the giving
     * macRaw address.
     *
     * @param hnc Set containing all HostNodeConnector to search.
     * @param macRaw The Mac address to search for.
     * @return The respective HostNodeConnector that contains the given macRaw,
     * null if wasn't found.
     */
    private HostNodeConnector getHostNodeConnectorFromMacAddress(Set<HostNodeConnector> hnc, byte[] macRaw) {
        HostNodeConnector foundHost = null;
        for (HostNodeConnector nc : hnc) {
            if (Arrays.equals(nc.getDataLayerAddressBytes(), macRaw)) {
                foundHost = nc;
                break;
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
        dstNotificationThread.get(keys).release();
    }

}
