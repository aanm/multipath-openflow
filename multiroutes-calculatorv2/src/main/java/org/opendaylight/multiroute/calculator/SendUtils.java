package org.opendaylight.multiroute.calculator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.opendaylight.controller.md.sal.common.api.TransactionStatus;
import org.opendaylight.controller.sal.binding.api.data.DataBrokerService;
import org.opendaylight.controller.sal.binding.api.data.DataModificationTransaction;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.core.Path;
import org.opendaylight.multiroute.cache.SrcDstPair;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowCapableNode;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.Table;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.TableKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.Flow;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.FlowModFlags;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.groups.Group;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.groups.GroupBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.Nodes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.NodeBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.NodeKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendUtils {

    private final static int flow_timeout = 30;
    private final Logger logger = LoggerFactory.getLogger(SendUtils.class);
    private final AtomicLong flowIdInc = new AtomicLong();
    private PacketProcessingService packetProcessingService;
    private DataBrokerService dataBrokerService;
    private final Cache ca = Cache.getCache();

    public void setDataBrokerService(DataBrokerService dataBrokerService) {
        this.dataBrokerService = dataBrokerService;
    }

    public void setPacketProcessingService(PacketProcessingService packetProcessingService) {
        this.packetProcessingService = packetProcessingService;
    }

    public Future<RpcResult<TransactionStatus>> writeFlowToNode(org.opendaylight.controller.sal.core.Node node, Flow flowBody) {
        NodeId nId = new NodeId(node.getNodeIDString());
        TableKey tk = new TableKey((short) 0);
        return writeFlowToNode(nId, tk, flowBody);
    }

    /**
     * Writes all the groups in the given gbs list in the respective node with
     * the given NodeId.
     *
     * @param nId The nodeId to set the given groups.
     * @param gbs The list of groups to write in the node.
     * @return A list with every transaction status for the respective commit of
     * all the given groups.
     */
    private List<Future<RpcResult<TransactionStatus>>> writeMultiGroupToNode(NodeId nId, List<GroupBuilder> gbs) {
        List<Future<RpcResult<TransactionStatus>>> listTransactionStatus = new ArrayList<>();
        InstanceIdentifier<Node> instIdNode = InstanceIdentifierUtils.createNodePath(nId);
        NodeKey nodeKey = instIdNode.firstKeyOf(Node.class, NodeKey.class);
        NodeBuilder builder = new NodeBuilder();
        builder.setId(nId);
        builder.setKey(nodeKey);
        Node node = builder.build();

        for (GroupBuilder gb : gbs) {
            if (ca.getInstanceIdentifier(nId, gb.getGroupId()) == null) {
                DataModificationTransaction addGroupTransation = dataBrokerService.beginTransaction();
                InstanceIdentifier<Group> pathGroup = InstanceIdentifier.builder(Nodes.class)
                        .child(Node.class, node.getKey()).augmentation(FlowCapableNode.class)
                        .child(Group.class, gb.getKey())
                        .build();
                addGroupTransation.putConfigurationData(instIdNode, node);
                addGroupTransation.putConfigurationData(pathGroup, gb.build());
                ca.saveInstanceIdentifier(nId, gb.getGroupId(), pathGroup);
                listTransactionStatus.add(addGroupTransation.commit());
                logger.debug("Commiting group for {} with config {}", nId, gb.build());
            }
        }
        return listTransactionStatus;
    }

    /**
     * Writes all the flows in the given fbs list in the respective node with
     * the given NodeId.
     *
     * @param nId The nodeId to set the given flows.
     * @param tk The tableKey where the nodes will be inserted.
     * @param fbs The list of flows to sent to the nodeId.
     * @return A list with every transaction status for the respective commit of
     * all the given flows.
     */
    private List<Future<RpcResult<TransactionStatus>>>
            rewriteMultiFlowToNode(NodeId nId, TableKey tk, List<FlowBuilder> fbs) {
        List<Future<RpcResult<TransactionStatus>>> listTransactionStatus = new ArrayList<>();
        InstanceIdentifier<Node> instIdNode
                = InstanceIdentifierUtils.createNodePath(nId);
        InstanceIdentifier<Table> instIdTable = InstanceIdentifierUtils.createTablePath(instIdNode, tk);
        for (FlowBuilder f : fbs) {
            /**
             * This means that the switch doesn't have this flow. And we have to
             * be careful because the old flow could have a same match but
             * different output.
             */
            FlowId flowId = new FlowId(String.valueOf(flowIdInc.getAndIncrement()));
            FlowKey flowKey = new FlowKey(flowId);
            InstanceIdentifier<Flow> instIdFlow = InstanceIdentifierUtils.createFlowPath(instIdTable, flowKey);
            ca.saveInstanceIdentifier(f.getCookie().longValue(), instIdFlow);
            ca.saveCookie(f.getMatch(), f.getCookie().longValue());
            DataModificationTransaction addFlowTransaction = this.dataBrokerService.beginTransaction();
            addFlowTransaction.putConfigurationData(instIdFlow, f.build());
            listTransactionStatus.add(addFlowTransaction.commit());
            logger.debug("Commiting flow for {} with config {}", nId, f.build());
        }
        return listTransactionStatus;
    }

    /**
     * Writes all the flows in the given fbs list in the respective node with
     * the given NodeId.
     *
     * @param nId The nodeId to set the given flows.
     * @param tk The tableKey where the nodes will be inserted.
     * @param fbs The list of flows to sent to the nodeId.
     * @return A list with every transaction status for the respective commit of
     * all the given flows.
     */
    private List<Future<RpcResult<TransactionStatus>>>
            writeMultiFlowToNode(NodeId nId, TableKey tk, List<FlowBuilder> fbs) {
        List<Future<RpcResult<TransactionStatus>>> listTransactionStatus = new ArrayList<>();
        InstanceIdentifier<Node> instIdNode
                = InstanceIdentifierUtils.createNodePath(nId);
        InstanceIdentifier<Table> instIdTable = InstanceIdentifierUtils.createTablePath(instIdNode, tk);
        for (FlowBuilder f : fbs) {
            /**
             * This means that the switch doesn't have this flow. And we have to
             * be careful because the old flow could have a same match but
             * different output.
             */
            Long oldCookie = ca.getCookieFromMatch(f.getMatch());
            long newCookie = f.getCookie().longValue();
            if (oldCookie == null || oldCookie != newCookie) {
                FlowId flowId = new FlowId(String.valueOf(flowIdInc.getAndIncrement()));
                FlowKey flowKey = new FlowKey(flowId);
                InstanceIdentifier<Flow> instIdFlow = InstanceIdentifierUtils.createFlowPath(instIdTable, flowKey);
                ca.saveInstanceIdentifier(f.getCookie().longValue(), instIdFlow);
                ca.saveCookie(f.getMatch(), f.getCookie().longValue());
                DataModificationTransaction addFlowTransaction = this.dataBrokerService.beginTransaction();
                addFlowTransaction.putConfigurationData(instIdFlow, f.build());
                listTransactionStatus.add(addFlowTransaction.commit());
                logger.debug("Commiting flow for {} with config {}", nId, f.build());
            }
        }
        return listTransactionStatus;
    }

    private Future<RpcResult<TransactionStatus>>
            writeFlowToNode(NodeId nodeId, TableKey tk, Flow flowBody) {
        FlowId flowId = new FlowId(String.valueOf(flowIdInc.getAndIncrement()));
        FlowKey flowKey = new FlowKey(flowId);
        DataModificationTransaction addFlowTransaction = this.dataBrokerService.beginTransaction();
        InstanceIdentifier<Node> instIdNode
                = InstanceIdentifierUtils.createNodePath(nodeId);
        InstanceIdentifier<Table> instIdTable = InstanceIdentifierUtils.createTablePath(instIdNode, tk);
        InstanceIdentifier<Flow> instIdFlow = InstanceIdentifierUtils.createFlowPath(instIdTable, flowKey);
        ca.saveInstanceIdentifier(flowBody.getCookie().longValue(), instIdFlow);
        ca.saveCookie(flowBody.getMatch(), flowBody.getCookie().longValue());
        addFlowTransaction.putConfigurationData(instIdFlow, flowBody);
        logger.debug("Commiting flow for {} with config {}", nodeId, flowBody);
        return addFlowTransaction.commit();
    }

    private NodeConnectorId nodeConnectorId(InstanceIdentifier<org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node> d, String connectorId) {
        NodeKey nodeKey = d.firstKeyOf(org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node.class, NodeKey.class);
        String stringId = nodeKey.getId().getValue() + ":" + connectorId;
        return new NodeConnectorId(stringId);
    }

    public Future<RpcResult<TransactionStatus>> programNode(NodeConnector srcNodeConnector,
                                                            NodeConnector dstNodeConnector,
                                                            MacAddress srcMac,
                                                            MacAddress dstMac,
                                                            PacketReceived notification) {
        SrcDstPair sdp = new SrcDstPair(srcMac, dstMac, srcNodeConnector, dstNodeConnector);

        FlowBuilder fb = FlowUtils.createEthFlowOutPort(srcNodeConnector, dstNodeConnector, srcMac, dstMac, flow_timeout);
        if (notification != null) {
            fb.setBufferId(notification.getBufferId().getValue());
        }

        ca.insertFlow(fb, sdp);
        return writeFlowToNode(srcNodeConnector.getNode(), fb.build());

    }

    public List<Future<RpcResult<TransactionStatus>>>
            programNodes(NodeConnector srcNodeConnector,
                         NodeConnector dstNodeConnector,
                         MacAddress srcMac,
                         MacAddress dstMac,
                         Path primaryRoute,
                         HashMap<NodeConnector, List<Path>> routeWithoutSingleEdges,
                         PacketReceived notification,
                         boolean configureRoundTrip) {
        List<Future<RpcResult<TransactionStatus>>> arrayTransactionStatus = new ArrayList<>();
        HashMap<org.opendaylight.controller.sal.core.Node, HashSet<SrcDstPair>> pairsOrigDst
                = new HashMap<>();

        SrcDstPair srcDstPair = new SrcDstPair(srcMac, dstMac, srcNodeConnector, dstNodeConnector);

        ca.insertPath(srcDstPair, primaryRoute);
        ca.insertEdgesFor(srcDstPair, primaryRoute);
        ca.saveEdgesFor(srcDstPair, routeWithoutSingleEdges.values());

        /**
         * Generate primary route's flows.
         */
        HashMap<org.opendaylight.controller.sal.core.Node, HashSet<SrcDstPair>> primaryPairsOrigDst
                = createSrcDstPairsFromPath(srcNodeConnector, dstNodeConnector, primaryRoute, srcMac, dstMac);

        HashMapUtils.putAllMultiValueHashSet(pairsOrigDst, primaryPairsOrigDst);

        /**
         * Adding the primary path to the routes without single edges because
         * now this is going to generate all the flows.
         */
        HashMapUtils.putMultiValue(routeWithoutSingleEdges, srcNodeConnector, primaryRoute);
        /**
         * Generate all flows.
         */
        for (Map.Entry<NodeConnector, List<Path>> nodes : routeWithoutSingleEdges.entrySet()) {
            for (Path p : nodes.getValue()) {
                HashMap<org.opendaylight.controller.sal.core.Node, HashSet<SrcDstPair>> tempSrcDstPair
                        = createSrcDstPairsFromPath(nodes.getKey(), dstNodeConnector, p, srcMac, dstMac);

                HashMapUtils.putAllMultiValueHashSet(pairsOrigDst, tempSrcDstPair);
            }
        }

        /**
         * Remove primary route's flows.
         */
        HashMapUtils.removeAllMultiValueHashSet(pairsOrigDst, primaryPairsOrigDst);

        HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> backuprouteWithgroups = new HashMap<>();
        /**
         * Create groups.
         */
        HashMap<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> groupsFromPairOrigDst
                = FlowUtils.createGroupsFromPairsOrigDst(primaryRoute, primaryPairsOrigDst, pairsOrigDst, backuprouteWithgroups);

        arrayTransactionStatus.addAll(sendGroupsToNodes(groupsFromPairOrigDst));

        /**
         * Create backup route's flows.
         */
        HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> backupFlows
                = FlowUtils.createEthFromPairOrigDst(pairsOrigDst);

        FlowBuilder firstFlow = getFirstFlow(srcNodeConnector, backupFlows);

        /**
         * Create primary route's flows.
         */
        HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsWithGroups;
        flowsWithGroups = FlowUtils.createEthFlowsFromPathWithGroupsNew(srcNodeConnector,
                                                                        dstNodeConnector,
                                                                        primaryRoute,
                                                                        srcMac,
                                                                        dstMac,
                                                                        groupsFromPairOrigDst);

        if (firstFlow == null) {
            firstFlow = getFirstFlow(srcNodeConnector, flowsWithGroups);
        }

        HashMapUtils.putAllMultiValue(flowsWithGroups, backupFlows);

        ca.insertFlows(flowsWithGroups, srcDstPair);
        
        ca.insertFlows(backuprouteWithgroups, srcDstPair);

        arrayTransactionStatus.addAll(sendFlowsToNodes(primaryRoute, flowsWithGroups));

        arrayTransactionStatus.addAll(sendFlowsToNodes(flowsWithGroups));

        arrayTransactionStatus.addAll(sendFlowsToNodes(backuprouteWithgroups));

        if (notification != null) {
            firstFlow.setBufferId(notification.getBufferId().getValue());
        }
        firstFlow.setFlags(new FlowModFlags(false, false, false, false, true));
        firstFlow.setIdleTimeout(flow_timeout);

        ca.insertFlow(firstFlow, srcDstPair);

        arrayTransactionStatus.add(writeFlowToNode(srcNodeConnector.getNode(), firstFlow.build()));

        logger.info("All principal nodes are set with flows with groups");

        return arrayTransactionStatus;
    }

    public List<Future<RpcResult<TransactionStatus>>>
            sendFlowsToNodes(HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsWithGroups) {
        List<Future<RpcResult<TransactionStatus>>> arrayTransactionStatus = new ArrayList<>();
        for (Map.Entry<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsOfNode : flowsWithGroups.entrySet()) {
            NodeId nId = new NodeId(flowsOfNode.getKey().getNodeIDString());
            TableKey tk = new TableKey((short) 0);
            arrayTransactionStatus.addAll(writeMultiFlowToNode(nId, tk, flowsOfNode.getValue()));
        }
        return arrayTransactionStatus;
    }

    public List<Future<RpcResult<TransactionStatus>>>
            sendFlowsModToNodes(HashSet<Long> flowsIdToDel) {
        List<Future<RpcResult<TransactionStatus>>> arrayTransactionStatus = new ArrayList<>();
        Iterator<Long> iterator = flowsIdToDel.iterator();
        DataModificationTransaction addFlowTransaction = this.dataBrokerService.beginTransaction();
        while (iterator.hasNext()) {
            Long next = iterator.next();
            InstanceIdentifier<Flow> iif = ca.getInstanceIdentifier(next);
            if (iif != null) {
                addFlowTransaction.removeConfigurationData(iif);
                addFlowTransaction.removeOperationalData(iif);
                logger.debug("Found flow cookie {} removing", next);
            }
            logger.debug("Removed flow with cookie {}", next);
        }
        arrayTransactionStatus.add(addFlowTransaction.commit());
        return arrayTransactionStatus;
    }

    public List<Future<RpcResult<TransactionStatus>>> sendFlowsToNodes(Path route, HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsWithGroups) {
        List<Future<RpcResult<TransactionStatus>>> arrayTransactionStatus = new ArrayList<>();
        Path reverse = route.reverse();
        for (org.opendaylight.controller.sal.core.Node node : reverse.getNodes()) {
            if (flowsWithGroups.containsKey(node)) {
                NodeId nId = new NodeId(node.getNodeIDString());
                TableKey tk = new TableKey((short) 0);
                arrayTransactionStatus.addAll(writeMultiFlowToNode(nId, tk, flowsWithGroups.remove(node)));
            }
        }
        return arrayTransactionStatus;
    }

    private List<Future<RpcResult<TransactionStatus>>> sendGroupsToNodes(HashMap<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> fbgrps) {
        List<Future<RpcResult<TransactionStatus>>> arrayTransactionStatus = new ArrayList<>();
        for (Map.Entry<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> nodes : fbgrps.entrySet()) {
            org.opendaylight.controller.sal.core.Node node = nodes.getKey();
            NodeId nId = new NodeId(node.getNodeIDString());
            arrayTransactionStatus.addAll(writeMultiGroupToNode(nId, nodes.getValue()));
        }
        return arrayTransactionStatus;
    }

    private static HashMap<org.opendaylight.controller.sal.core.Node, HashSet<SrcDstPair>>
            createSrcDstPairsFromPath(NodeConnector srcNodeConnector,
                                      NodeConnector dstNodeConnector,
                                      Path route,
                                      MacAddress srcMac,
                                      MacAddress dstMac) {
        HashMap<org.opendaylight.controller.sal.core.Node, HashSet<SrcDstPair>> flows = new HashMap<>();
        List<Edge> edges = route.getEdges();

        HashMapUtils.//
                putMultiValueHashSet(flows,//
                                     srcNodeConnector.getNode(),//
                                     new SrcDstPair(srcMac, dstMac, srcNodeConnector, edges.get(0).getTailNodeConnector()));
        int i;
        for (i = 1; i < edges.size(); i++) {
            HashMapUtils.//
                    putMultiValueHashSet(flows,//
                                         edges.get(i - 1).getHeadNodeConnector().getNode(),//
                                         new SrcDstPair(srcMac,
                                                        dstMac,
                                                        edges.get(i - 1).getHeadNodeConnector(),
                                                        edges.get(i).getTailNodeConnector()));
        }
        HashMapUtils.//
                putMultiValueHashSet(flows,//
                                     edges.get(i - 1).getHeadNodeConnector().getNode(),//
                                     new SrcDstPair(srcMac,
                                                    dstMac,
                                                    edges.get(i - 1).getHeadNodeConnector(),
                                                    dstNodeConnector));
        return flows;
    }

    private static FlowBuilder getFirstFlow(NodeConnector srcNodeConnector,
                                            HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> backupFlows) {
        List<FlowBuilder> flowBuildersFromSrcNode = backupFlows.get(srcNodeConnector.getNode());
        FlowBuilder firstFlow = null;
        if (flowBuildersFromSrcNode != null && !flowBuildersFromSrcNode.isEmpty()) {
            for (FlowBuilder fb : flowBuildersFromSrcNode) {
                if (fb.getMatch().getInPort().getValue().equals(srcNodeConnector.getID())) {
                    flowBuildersFromSrcNode.remove(fb);
                    firstFlow = fb;
                    break;
                }
            }
        }
        return firstFlow;
    }

    public List<Future<RpcResult<TransactionStatus>>> sendFlowsToNode(org.opendaylight.controller.sal.core.Node sourceNode, List<FlowBuilder> lFbs) {
        List<Future<RpcResult<TransactionStatus>>> arrayTransactionStatus = new ArrayList<>();
        NodeId nId = new NodeId(sourceNode.getNodeIDString());
        TableKey tk = new TableKey((short) 0);
        arrayTransactionStatus.addAll(writeMultiFlowToNode(nId, tk, lFbs));
        return arrayTransactionStatus;
    }

    List<Future<RpcResult<TransactionStatus>>> resendFlowsToNode(org.opendaylight.controller.sal.core.Node sourceNode, List<FlowBuilder> lFbs) {
        List<Future<RpcResult<TransactionStatus>>> arrayTransactionStatus = new ArrayList<>();
        NodeId nId = new NodeId(sourceNode.getNodeIDString());
        TableKey tk = new TableKey((short) 0);
        arrayTransactionStatus.addAll(rewriteMultiFlowToNode(nId, tk, lFbs));
        return arrayTransactionStatus;
    }

}
