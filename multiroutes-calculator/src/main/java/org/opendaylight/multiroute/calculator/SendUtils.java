package org.opendaylight.multiroute.calculator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.opendaylight.controller.md.sal.common.api.TransactionStatus;
import org.opendaylight.controller.sal.binding.api.data.DataBrokerService;
import org.opendaylight.controller.sal.binding.api.data.DataModificationTransaction;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.core.Path;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowCapableNode;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.Table;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.TableKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.Flow;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.groups.Group;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.groups.GroupBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.groups.GroupKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.Nodes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnectorKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.NodeBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.NodeKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.TransmitPacketInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.TransmitPacketInputBuilder;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendUtils {

    private final Logger logger = LoggerFactory.getLogger(SendUtils.class);
    private final AtomicLong flowIdInc = new AtomicLong();
    private PacketProcessingService packetProcessingService;
    private DataBrokerService dataBrokerService;
    private final Cache ca = Cache.getCache();

    @Deprecated
    public Future<RpcResult<TransactionStatus>> writeGroupToNode(NodeId nId, Group g) {
        logger.debug("Writing group in config");
        InstanceIdentifier<Node> instIdNode
                = InstanceIdentifierUtils.createNodePath(nId);
        NodeKey nodeKey = instIdNode.firstKeyOf(Node.class, NodeKey.class);
        NodeBuilder builder = new NodeBuilder();
        builder.setId(nId);
        builder.setKey(nodeKey);
        Node node = builder.build();

        DataModificationTransaction addGroupTransation = dataBrokerService.beginTransaction();
        InstanceIdentifier<Group> path1 = InstanceIdentifier.builder(Nodes.class)
                .child(Node.class, node.getKey()).augmentation(FlowCapableNode.class)
                .child(Group.class, new GroupKey(g.getGroupId()))
                .build();
        addGroupTransation.putConfigurationData(instIdNode, node);
        addGroupTransation.putConfigurationData(path1, g);
        logger.debug("Commiting group in config");
        return addGroupTransation.commit();
    }

    /**
     *
     * @param sourceNode
     * @param srcNodeConnector
     * @param destinationNode
     * @param dstNodeConnector
     * @param srcMac
     * @param dstMac
     * @param route
     * @param backupRoute
     * @param notification
     * @param configureRoundTrip If true, nodes will be configured with a flow
     * for the path from source to destination and a flow for the path from
     * destination to source at the same time. If false the nodes will be
     * configured with a flow only from the source to the destination.
     * @return
     */
    public List<Future<RpcResult<TransactionStatus>>> sendFlowsForRoute(org.opendaylight.controller.sal.core.Node sourceNode,
                                                                        NodeConnector srcNodeConnector,
                                                                        org.opendaylight.controller.sal.core.Node destinationNode,
                                                                        NodeConnector dstNodeConnector,
                                                                        MacAddress srcMac,
                                                                        MacAddress dstMac,
                                                                        Path route,
                                                                        Path backupRoute,
                                                                        PacketReceived notification,
                                                                        boolean configureRoundTrip) {
        List<Future<RpcResult<TransactionStatus>>> arrayTransactionStatus = new ArrayList<>();
        if (route != null) {
            HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsWithGroups;
            FlowBuilder firstFlow;
            if (backupRoute != null) {
                /**
                 * First we create wayback groups.
                 */
                HashMap<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> fbgrps
                        = FlowUtils.createWayBackGroupsFromPath(srcNodeConnector,
                                                                dstNodeConnector,
                                                                route,
                                                                backupRoute);
                HashMap<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> originalFbgrps
                        = new HashMap<>(fbgrps);
                HashMap<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> fbgrpRoundTrip = null;
                if (configureRoundTrip) {
                    fbgrpRoundTrip = FlowUtils.createWayBackGroupsFromPath(dstNodeConnector,
                                                                           srcNodeConnector,
                                                                           route.reverse(),
                                                                           backupRoute.reverse());
                    HashMapUtils.putAllMultiValue(fbgrps, fbgrpRoundTrip);
                    logger.debug("Configuring principal route with round trip groups");
                }

                /**
                 * And send them to the nodes.
                 */
                for (Map.Entry<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> nodes : fbgrps.entrySet()) {
                    org.opendaylight.controller.sal.core.Node node = nodes.getKey();
                    NodeId nId = new NodeId(node.getNodeIDString());
                    arrayTransactionStatus.addAll(writeMultiGroupToNode(nId, nodes.getValue()));
                }
                logger.info("All principal route's nodes are set with group flows");

                /**
                 * Now we create the flows to output the matches to the groups.
                 */
                flowsWithGroups = ca.getEthFlowsFromPath(route,
                                                         srcMac,
                                                         dstMac,
                                                         Cache.FlowType.NORMAL);
                if (flowsWithGroups == null) {
                    flowsWithGroups = FlowUtils.createEthFlowsFromPathWithGroups(srcNodeConnector,
                                                                                 route,
                                                                                 srcMac,
                                                                                 dstMac,
                                                                                 originalFbgrps);

                    ca.insertFlows(flowsWithGroups, srcMac, dstMac, Cache.FlowType.NORMAL);
                }


                /**
                 * Second we create the experimental messages.
                 */
                HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsExperimental
                        = ca.getEthFlowsFromPath(route, srcMac, dstMac, Cache.FlowType.EXPERIMENTAL);
                if (flowsExperimental == null) {
                    flowsExperimental
                            = FlowUtils.createExpFlowsFromPath(srcNodeConnector,
                                                               route,
                                                               backupRoute,
                                                               srcMac,
                                                               dstMac,
                                                               flowsWithGroups);
                    ca.insertFlows(flowsExperimental, srcMac, dstMac, Cache.FlowType.EXPERIMENTAL);
                }
                
                /**
                 * We set the bufferId for the match that trigger all this.
                 */
                firstFlow = flowsWithGroups.get(srcNodeConnector.getNode()).remove(0);
                firstFlow.setBufferId(notification.getBufferId().getValue());

                if (configureRoundTrip) {
                    HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsWithGroupsRoundTrip
                            = ca.getEthFlowsFromPath(route.reverse(),
                                                     dstMac,
                                                     srcMac,
                                                     Cache.FlowType.NORMAL);
                    if (flowsWithGroupsRoundTrip == null) {
                        flowsWithGroupsRoundTrip = FlowUtils.createEthFlowsFromPathWithGroups(dstNodeConnector,
                                                                                              route.reverse(),
                                                                                              dstMac,
                                                                                              srcMac,
                                                                                              fbgrpRoundTrip);

                        ca.insertFlows(flowsWithGroupsRoundTrip, dstMac, srcMac, Cache.FlowType.NORMAL);
                    }
                    HashMapUtils.putAllMultiValue(flowsWithGroups, flowsWithGroupsRoundTrip);
                    logger.debug("Configuring principal route with round trip flows with groups");

                    HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsExperimentalRoundTrip
                            = ca.getEthFlowsFromPath(route.reverse(), dstMac, srcMac, Cache.FlowType.EXPERIMENTAL);
                    if (flowsExperimentalRoundTrip == null) {
                        flowsExperimentalRoundTrip = FlowUtils.createExpFlowsFromPath(dstNodeConnector,
                                                                                      route.reverse(),
                                                                                      backupRoute.reverse(),
                                                                                      dstMac,
                                                                                      srcMac,
                                                                                      flowsWithGroupsRoundTrip);
                        ca.insertFlows(flowsExperimentalRoundTrip, dstMac, srcMac, Cache.FlowType.EXPERIMENTAL);
                    }
                    HashMapUtils.putAllMultiValue(flowsExperimental, flowsExperimentalRoundTrip);
                    logger.debug("Configuring principal route with round trip experimental");
                }

                /**
                 * And send the experimental flows to the nodes.
                 */
                for (Map.Entry<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> nodes : flowsExperimental.entrySet()) {
                    org.opendaylight.controller.sal.core.Node node = nodes.getKey();
                    NodeId nId = new NodeId(node.getNodeIDString());
                    TableKey tk = new TableKey((short) 0);
                    arrayTransactionStatus.addAll(writeMultiFlowToNode(nId, tk, nodes.getValue()));
                }
                logger.info("All principal nodes are set with experimental flows");
            } else {

                flowsWithGroups = ca.getEthFlowsFromPath(route,
                                                         srcMac,
                                                         dstMac,
                                                         Cache.FlowType.NORMAL);
                if (flowsWithGroups == null) {
                    flowsWithGroups = FlowUtils.createEthFlowsFromPath(srcNodeConnector,
                                                                       dstNodeConnector,
                                                                       route,
                                                                       srcMac,
                                                                       dstMac);

                    ca.insertFlows(flowsWithGroups, srcMac, dstMac, Cache.FlowType.NORMAL);
                }

                firstFlow = flowsWithGroups.get(srcNodeConnector.getNode()).remove(0);
                firstFlow.setBufferId(notification.getBufferId().getValue());

                if (configureRoundTrip) {
                    HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsWithGroupsRoundTrip
                            = FlowUtils.createEthFlowsFromPath(dstNodeConnector,
                                                               srcNodeConnector,
                                                               route.reverse(),
                                                               dstMac,
                                                               srcMac);
                    HashMapUtils.putAllMultiValue(flowsWithGroups, flowsWithGroupsRoundTrip);
                    ca.insertFlows(flowsWithGroupsRoundTrip, dstMac, srcMac, Cache.FlowType.NORMAL);
                    logger.debug("Configuring principal route with round trip flows");
                }
            }
            /**
             * And finally send all the flows to the nodes in the reverse order
             * of the path. This way we prevent for example, the 2nd node to
             * contact the controller for a match if the 1st node had already
             * received it's flow configuration.
             */
            flowsWithGroups.get(srcNodeConnector.getNode()).add(firstFlow);
            Path reverse = route.reverse();
            for (org.opendaylight.controller.sal.core.Node node : reverse.getNodes()) {
                if (flowsWithGroups.containsKey(node)) {
                    NodeId nId = new NodeId(node.getNodeIDString());
                    TableKey tk = new TableKey((short) 0);
                    arrayTransactionStatus.addAll(writeMultiFlowToNode(nId, tk, flowsWithGroups.get(node)));
                }
            }
            logger.info("All principal nodes are set with flows with groups");

        } else {
            //Since the IRouting algorithm gives a null route between the same
            //node we've to create the flow rule for this single node.
            if (sourceNode.equals(destinationNode)) {
//                FlowBuilder fb = FlowUtils.createEthFlowOutPort(srcNodeConnector, dstNodeConnector, srcMac, dstMac, 10);
//                //Set the buffer id only for the sourceNode
//                if (originToDestiny) {
//                    fb.setBufferId(notification.getBufferId().getValue());
//                }
//                NodeId nId = new NodeId(sourceNode.getNodeIDString());
//                TableKey tk = new TableKey((short) 0);
//                Flow f = fb.build();
//                sendU.writeFlowToConfig(nId, tk, f);
//                logger.debug("Flow {} sent to the node {}",
//                             f.toString(),
//                             sourceNode.toString());
                //We don't want to miss any packet so send it alread for the destination
//                sendU.sendPacketOut(notification.getPayload(), srcNodeConnector);
            } else {
                logger.info("Route between {} and {} was not found", sourceNode.toString(), destinationNode.toString());
            }
        }
        return arrayTransactionStatus;
    }

    @Deprecated
    public void sendPacketOut(byte[] data, NodeConnector nodeConnector) {
        NodeId ni = new NodeId(nodeConnector.getNode().getNodeIDString());
        InstanceIdentifier<org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node> node//
                = InstanceIdentifierUtils.createNodePath(ni);
        NodeConnectorKey nodeConnectorKey//
                = new NodeConnectorKey(nodeConnectorId(node, nodeConnector.getNodeConnectorIdAsString()));
        InstanceIdentifier<?> nodeConnectorPath//
                = InstanceIdentifierUtils.createNodeConnectorPath(node, nodeConnectorKey);
        NodeConnectorRef egressConnectorRef//
                = new NodeConnectorRef(nodeConnectorPath);

        sendPacketOut(data, egressConnectorRef);
    }

    @Deprecated
    public void
            sendPacketOut(byte[] payload, NodeConnectorRef egress) {
        InstanceIdentifier<org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node> egressNodePath = InstanceIdentifierUtils.getNodePath(egress.getValue());
        TransmitPacketInput input = new TransmitPacketInputBuilder() //
                .setPayload(payload) //
                .setNode(new NodeRef(egressNodePath)) //
                .setEgress(egress) //
                .build();
        packetProcessingService.transmitPacket(input);

    }

    public void setDataBrokerService(DataBrokerService dataBrokerService) {
        this.dataBrokerService = dataBrokerService;
    }

    public void setPacketProcessingService(PacketProcessingService packetProcessingService) {
        this.packetProcessingService = packetProcessingService;
    }

    /**
     * Creates and sends all the flows to the nodes in the backup route except
     * the source node. The source node will have is flow created by the primary
     * route with the output group.
     *
     * @param srcNodeConnector
     * @param dstNodeConnector
     * @param srcMac
     * @param dstMac
     * @param backupRoute
     * @param configureRoundTrip If true, nodes will be configured with a flow
     * for the path from source to destination and a flow for the path from
     * destination to source at the same time. If false the nodes will be
     * configured with a flow only from the source to the destination.
     * @return A list with all the TransactionStatus for the all the flow sent
     * for all the nodes in the given backupRoute.
     */
    public List<Future<RpcResult<TransactionStatus>>> sendFlowsForBackupRoute(NodeConnector srcNodeConnector, NodeConnector dstNodeConnector, MacAddress srcMac, MacAddress dstMac, Path backupRoute, boolean configureRoundTrip) {

        List<Future<RpcResult<TransactionStatus>>> arrayTransactionStatus
                = new ArrayList<>();
        if (backupRoute != null) {
            HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> fbs
                    = ca.getEthFlowsFromPath(backupRoute,
                                             srcMac,
                                             dstMac,
                                             Cache.FlowType.BACKUP);
            if (fbs == null) {
                fbs = FlowUtils.createEthFlowsFromBackupPath(dstNodeConnector,
                                                             backupRoute,
                                                             srcMac,
                                                             dstMac);
                ca.insertFlows(fbs, srcMac, dstMac, Cache.FlowType.BACKUP);
            }
            if (configureRoundTrip) {
                HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> fbRoundTrip
                        = ca.getEthFlowsFromPath(backupRoute.reverse(),
                                                 dstMac,
                                                 srcMac,
                                                 Cache.FlowType.BACKUP);
                if (fbRoundTrip == null) {
                    fbRoundTrip
                            = FlowUtils.createEthFlowsFromBackupPath(srcNodeConnector,
                                                                     backupRoute.reverse(),
                                                                     dstMac,
                                                                     srcMac);
                    ca.insertFlows(fbRoundTrip, dstMac, srcMac, Cache.FlowType.BACKUP);
                }
                HashMapUtils.putAllMultiValue(fbs, fbRoundTrip);
                logger.debug("Configuring backup with round trip");
            }
            for (Map.Entry<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> nodes : fbs.entrySet()) {
                org.opendaylight.controller.sal.core.Node node = nodes.getKey();
                NodeId nId = new NodeId(node.getNodeIDString());
                TableKey tk = new TableKey((short) 0);
                arrayTransactionStatus.addAll(writeMultiFlowToNode(nId, tk, nodes.getValue()));
            }
            logger.info("All backup nodes are set with backup flows");
        }
        return arrayTransactionStatus;
    }

    public Future<RpcResult<TransactionStatus>> writeFlowToNode(org.opendaylight.controller.sal.core.Node node, Flow flowBody) {
        NodeId nId = new NodeId(node.getNodeIDString());
        TableKey tk = new TableKey((short) 0);
        return writeFlowToNode(nId, tk, flowBody);
    }

    /**
     * TODO: See if it's a good idea to receive a srcDstPair list
     *
     * @param srcEth
     * @param srcNodeConnector
     * @param dstEth
     * @param dstNodeConnector
     * @param pPath
     * @param bPath
     * @param configureRoundTrip
     * @return
     */
    public List<Future<RpcResult<TransactionStatus>>> configureCoreNodes(MacAddress srcEth, NodeConnector srcNodeConnector, MacAddress dstEth, NodeConnector dstNodeConnector, Path pPath, Path bPath, boolean configureRoundTrip) {
        List<Future<RpcResult<TransactionStatus>>> arrayTransactionStatus = new ArrayList<>();
        if (pPath != null) {
            HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsWithGroups;
            if (bPath != null) {
                /**
                 * First we create wayback groups.
                 */
                HashMap<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> fbgrps
                        = FlowUtils.createCoreWayBackGroupsFromPath(dstNodeConnector,
                                                                    pPath,
                                                                    bPath);

                HashMap<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> originalFbgrps
                        = new HashMap<>(fbgrps);
                HashMap<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> fbgrpRoundTrip = null;
                if (configureRoundTrip) {
                    fbgrpRoundTrip = FlowUtils.createCoreWayBackGroupsFromPath(srcNodeConnector,
                                                                               pPath.reverse(),
                                                                               bPath.reverse());
                    HashMapUtils.putAllMultiValue(fbgrps, fbgrpRoundTrip);
                    logger.debug("Configuring principal route with round trip groups");
                }

                /**
                 * And send them to the nodes.
                 */
                for (Map.Entry<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> nodes : fbgrps.entrySet()) {
                    org.opendaylight.controller.sal.core.Node node = nodes.getKey();
                    NodeId nId = new NodeId(node.getNodeIDString());
                    arrayTransactionStatus.addAll(writeMultiGroupToNode(nId, nodes.getValue()));
                }
                logger.info("All principal route's nodes are set with group flows");

                /**
                 * Now we create the flows to output the matches to the groups.
                 */
                flowsWithGroups = FlowUtils.createCoreEthFlowsFromPathWithGroups(pPath,
                                                                                 srcEth,
                                                                                 dstEth,
                                                                                 originalFbgrps);

                ca.insertFlows(flowsWithGroups, srcEth, dstEth, Cache.FlowType.NORMAL);

                /**
                 * Second we create the experimental messages.
                 */
                HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsExperimental
                        = FlowUtils.createCoreExpFlowsFromPath(srcNodeConnector,
                                                               pPath,
                                                               srcEth,
                                                               dstEth,
                                                               flowsWithGroups);
                ca.insertFlows(flowsExperimental, srcEth, dstEth, Cache.FlowType.EXPERIMENTAL);

                if (configureRoundTrip) {
                    HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsWithGroupsRoundTrip
                            = FlowUtils.createCoreEthFlowsFromPathWithGroups(pPath.reverse(),
                                                                             dstEth,
                                                                             srcEth,
                                                                             fbgrpRoundTrip);
                    ca.insertFlows(flowsWithGroupsRoundTrip, dstEth, srcEth, Cache.FlowType.NORMAL);
                    HashMapUtils.putAllMultiValue(flowsWithGroups, flowsWithGroupsRoundTrip);
                    logger.debug("Configuring principal route with round trip flows with groups");

                    HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsExperimentalRoundTrip
                            = FlowUtils.createCoreExpFlowsFromPath(dstNodeConnector,
                                                                   pPath.reverse(),
                                                                   dstEth,
                                                                   srcEth,
                                                                   flowsWithGroupsRoundTrip);
                    ca.insertFlows(flowsExperimentalRoundTrip, dstEth, srcEth, Cache.FlowType.EXPERIMENTAL);
                    HashMapUtils.putAllMultiValue(flowsExperimental, flowsExperimentalRoundTrip);
                    logger.debug("Configuring principal route with round trip experimental");
                }

                /**
                 * And send the experimental flows to the nodes.
                 */
                for (Map.Entry<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> nodes : flowsExperimental.entrySet()) {
                    org.opendaylight.controller.sal.core.Node node = nodes.getKey();
                    NodeId nId = new NodeId(node.getNodeIDString());
                    TableKey tk = new TableKey((short) 0);
                    arrayTransactionStatus.addAll(writeMultiFlowToNode(nId, tk, nodes.getValue()));
                }
                logger.info("All principal nodes are set with experimental flows");
            } else {
                flowsWithGroups = FlowUtils.createCoreEthFlowsFromPath(pPath,
                                                                       srcEth,
                                                                       dstEth);

                ca.insertFlows(flowsWithGroups, srcEth, dstEth, Cache.FlowType.NORMAL);

                if (configureRoundTrip) {
                    HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsWithGroupsRoundTrip
                            = FlowUtils.createCoreEthFlowsFromPath(pPath.reverse(),
                                                                   dstEth,
                                                                   srcEth);
                    HashMapUtils.putAllMultiValue(flowsWithGroups, flowsWithGroupsRoundTrip);
                    ca.insertFlows(flowsWithGroupsRoundTrip, dstEth, srcEth, Cache.FlowType.NORMAL);
                    logger.debug("Configuring principal route with round trip flows");
                }
            }
            /**
             * And finally send all the flows to the nodes in the reverse order
             * of the path. This way we prevent for example, the 2nd node to
             * contact the controller for a match if the 1st node had already
             * received it's flow configuration.
             */
            Path reverse = pPath.reverse();
            for (org.opendaylight.controller.sal.core.Node node : reverse.getNodes()) {
                if (flowsWithGroups.containsKey(node)) {
                    NodeId nId = new NodeId(node.getNodeIDString());
                    TableKey tk = new TableKey((short) 0);
                    arrayTransactionStatus.addAll(writeMultiFlowToNode(nId, tk, flowsWithGroups.get(node)));
                }
            }
            logger.info("All principal nodes are set with flows with groups");
        } else {
            //Since the IRouting algorithm gives a null route between the same
            //node we've to create the flow rule for this single node.
//            if (sourceNode.equals(destinationNode)) {
//                FlowBuilder fb = FlowUtils.createEthFlowOutPort(srcNodeConnector, dstNodeConnector, srcMac, dstMac, 10);
//                //Set the buffer id only for the sourceNode
//                if (originToDestiny) {
//                    fb.setBufferId(notification.getBufferId().getValue());
//                }
//                NodeId nId = new NodeId(sourceNode.getNodeIDString());
//                TableKey tk = new TableKey((short) 0);
//                Flow f = fb.build();
//                sendU.writeFlowToConfig(nId, tk, f);
//                logger.debug("Flow {} sent to the node {}",
//                             f.toString(),
//                             sourceNode.toString());
            //We don't want to miss any packet so send it alread for the destination
//                sendU.sendPacketOut(notification.getPayload(), srcNodeConnector);
//            } else {
//                logger.info("Route between {} and {} was not found", sourceNode.toString(), destinationNode.toString());
//            }
        }
        return arrayTransactionStatus;
    }

    /**
     * TODO: See if it's a good idea to receive a srcDstPair list
     *
     * @param srcEth
     * @param srcNodeConnector
     * @param dstEth
     * @param dstNodeConnector
     * @param pPath
     * @param bPath
     * @param configureRoundTrip
     * @return
     */
    public List<Future<RpcResult<TransactionStatus>>> configurePeripheralNodes(MacAddress srcEth, NodeConnector srcNodeConnector, MacAddress dstEth, NodeConnector dstNodeConnector, Path pPath, Path bPath, boolean configureRoundTrip) {
        List<Future<RpcResult<TransactionStatus>>> arrayTransactionStatus = new ArrayList<>();
        if (pPath != null) {
            HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsWithGroups;
            if (bPath != null) {
                /**
                 * First we create wayback groups.
                 */
                HashMap<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> fbgrps
                        = FlowUtils.createPeripheralWayBackGroupsFromPath(srcNodeConnector,
                                                                          dstNodeConnector,
                                                                          pPath,
                                                                          bPath);

                HashMap<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> originalFbgrps
                        = new HashMap<>(fbgrps);
                HashMap<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> fbgrpRoundTrip = null;
                if (configureRoundTrip) {
                    fbgrpRoundTrip = FlowUtils.createPeripheralWayBackGroupsFromPath(dstNodeConnector,
                                                                                     srcNodeConnector,
                                                                                     pPath.reverse(),
                                                                                     bPath.reverse());
                    HashMapUtils.putAllMultiValue(fbgrps, fbgrpRoundTrip);
                    logger.debug("Configuring principal route with round trip groups");
                }

                /**
                 * And send them to the nodes.
                 */
                for (Map.Entry<org.opendaylight.controller.sal.core.Node, List<GroupBuilder>> nodes : fbgrps.entrySet()) {
                    org.opendaylight.controller.sal.core.Node node = nodes.getKey();
                    NodeId nId = new NodeId(node.getNodeIDString());
                    arrayTransactionStatus.addAll(writeMultiGroupToNode(nId, nodes.getValue()));
                }
                logger.info("All principal route's nodes are set with group flows");

                /**
                 * Now we create the flows to output the matches to the groups.
                 */
                flowsWithGroups = FlowUtils.createPeripheralEthFlowsFromPathWithGroups(srcNodeConnector,
                                                                                       pPath,
                                                                                       srcEth,
                                                                                       dstEth,
                                                                                       originalFbgrps);

                ca.insertFlows(flowsWithGroups, srcEth, dstEth, Cache.FlowType.NORMAL);

                /**
                 * Second we create the experimental messages.
                 */
                HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsExperimental
                        = FlowUtils.createPeripheralExpFlowsFromPath(srcNodeConnector,
                                                                     pPath,
                                                                     bPath,
                                                                     srcEth,
                                                                     dstEth,
                                                                     flowsWithGroups);
                ca.insertFlows(flowsExperimental, srcEth, dstEth, Cache.FlowType.EXPERIMENTAL);

                if (configureRoundTrip) {
                    HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsWithGroupsRoundTrip
                            = FlowUtils.createPeripheralEthFlowsFromPathWithGroups(dstNodeConnector,
                                                                                   pPath.reverse(),
                                                                                   dstEth,
                                                                                   srcEth,
                                                                                   fbgrpRoundTrip);
                    ca.insertFlows(flowsWithGroupsRoundTrip, dstEth, srcEth, Cache.FlowType.NORMAL);
                    HashMapUtils.putAllMultiValue(flowsWithGroups, flowsWithGroupsRoundTrip);
                    logger.debug("Configuring principal route with round trip flows with groups");

                    HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsExperimentalRoundTrip
                            = FlowUtils.createPeripheralExpFlowsFromPath(dstNodeConnector,
                                                                         pPath.reverse(),
                                                                         bPath.reverse(),
                                                                         dstEth,
                                                                         srcEth,
                                                                         flowsWithGroupsRoundTrip);
                    ca.insertFlows(flowsExperimentalRoundTrip, dstEth, srcEth, Cache.FlowType.EXPERIMENTAL);
                    HashMapUtils.putAllMultiValue(flowsExperimental, flowsExperimentalRoundTrip);
                    logger.debug("Configuring principal route with round trip experimental");
                }

                /**
                 * And send the experimental flows to the nodes.
                 */
                for (Map.Entry<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> nodes : flowsExperimental.entrySet()) {
                    org.opendaylight.controller.sal.core.Node node = nodes.getKey();
                    NodeId nId = new NodeId(node.getNodeIDString());
                    TableKey tk = new TableKey((short) 0);
                    arrayTransactionStatus.addAll(writeMultiFlowToNode(nId, tk, nodes.getValue()));
                }
                logger.info("All principal nodes are set with experimental flows");

            } else {
                flowsWithGroups = FlowUtils.createPeripheralEthFlowsFromPath(srcNodeConnector,
                                                                             dstNodeConnector,
                                                                             pPath,
                                                                             srcEth,
                                                                             dstEth);

                ca.insertFlows(flowsWithGroups, srcEth, dstEth, Cache.FlowType.NORMAL);

                if (configureRoundTrip) {
                    HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsWithGroupsRoundTrip
                            = FlowUtils.createPeripheralEthFlowsFromPath(dstNodeConnector,
                                                                         srcNodeConnector,
                                                                         pPath.reverse(),
                                                                         dstEth,
                                                                         srcEth);
                    HashMapUtils.putAllMultiValue(flowsWithGroups, flowsWithGroupsRoundTrip);
                    ca.insertFlows(flowsWithGroupsRoundTrip, dstEth, srcEth, Cache.FlowType.NORMAL);
                    logger.debug("Configuring principal route with round trip flows");
                }
            }
            /**
             * And finally send all the flows to the nodes in the reverse order
             * of the path. This way we prevent for example, the 2nd node to
             * contact the controller for a match if the 1st node had already
             * received it's flow configuration.
             */
            Path reverse = pPath.reverse();
            for (org.opendaylight.controller.sal.core.Node node : reverse.getNodes()) {
                if (flowsWithGroups.containsKey(node)) {
                    NodeId nId = new NodeId(node.getNodeIDString());
                    TableKey tk = new TableKey((short) 0);
                    arrayTransactionStatus.addAll(writeMultiFlowToNode(nId, tk, flowsWithGroups.get(node)));
                }
            }
            logger.info("All principal nodes are set with flows with groups");
        } else {
            //Since the IRouting algorithm gives a null route between the same
            //node we've to create the flow rule for this single node.
//            if (sourceNode.equals(destinationNode)) {
//                FlowBuilder fb = FlowUtils.createEthFlowOutPort(srcNodeConnector, dstNodeConnector, srcMac, dstMac, 10);
//                //Set the buffer id only for the sourceNode
//                if (originToDestiny) {
//                    fb.setBufferId(notification.getBufferId().getValue());
//                }
//                NodeId nId = new NodeId(sourceNode.getNodeIDString());
//                TableKey tk = new TableKey((short) 0);
//                Flow f = fb.build();
//                sendU.writeFlowToConfig(nId, tk, f);
//                logger.debug("Flow {} sent to the node {}",
//                             f.toString(),
//                             sourceNode.toString());
            //We don't want to miss any packet so send it alread for the destination
//                sendU.sendPacketOut(notification.getPayload(), srcNodeConnector);
//            } else {
//                logger.info("Route between {} and {} was not found", sourceNode.toString(), destinationNode.toString());
//            }
        }
        return arrayTransactionStatus;
    }

    public List<Future<RpcResult<TransactionStatus>>> configureBackUpNodes(MacAddress srcMac, MacAddress dstMac, NodeConnector dstNodeConnector, Path backupPath, boolean configureRoundTrip) {

        List<Future<RpcResult<TransactionStatus>>> arrayTransactionStatus
                = new ArrayList<>();
        if (backupPath != null) {
            HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> fbs
                    = FlowUtils.createEthFlowsFromBackupPath(dstNodeConnector,
                                                             backupPath,
                                                             srcMac,
                                                             dstMac);
            ca.insertFlows(fbs, srcMac, dstMac, Cache.FlowType.BACKUP);
            if (configureRoundTrip) {
                HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> fbRoundTrip
                        = FlowUtils.createEthFlowsFromBackupPath(dstNodeConnector,
                                                                 backupPath.reverse(),
                                                                 dstMac,
                                                                 srcMac);
                HashMapUtils.putAllMultiValue(fbs, fbRoundTrip);
                ca.insertFlows(fbs, dstMac, srcMac, Cache.FlowType.BACKUP);
                logger.debug("Configuring backup with round trip");
            }

            for (Map.Entry<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> nodes : fbs.entrySet()) {
                org.opendaylight.controller.sal.core.Node node = nodes.getKey();
                NodeId nId = new NodeId(node.getNodeIDString());
                TableKey tk = new TableKey((short) 0);
                arrayTransactionStatus.addAll(writeMultiFlowToNode(nId, tk, nodes.getValue()));
            }
            logger.info("All backup nodes are set with backup flows");
        }
        return arrayTransactionStatus;
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
            DataModificationTransaction addGroupTransation = dataBrokerService.beginTransaction();
            InstanceIdentifier<Group> pathGroup = InstanceIdentifier.builder(Nodes.class)
                    .child(Node.class, node.getKey()).augmentation(FlowCapableNode.class)
                    .child(Group.class, gb.getKey())
                    .build();
            addGroupTransation.putConfigurationData(instIdNode, node);
            addGroupTransation.putConfigurationData(pathGroup, gb.build());
            listTransactionStatus.add(addGroupTransation.commit());
            logger.debug("Commiting group for {} with config {}", nId, gb.build());
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
            FlowId flowId = new FlowId(String.valueOf(flowIdInc.getAndIncrement()));
            FlowKey flowKey = new FlowKey(flowId);
            InstanceIdentifier<Flow> instIdFlow = InstanceIdentifierUtils.createFlowPath(instIdTable, flowKey);
            DataModificationTransaction addFlowTransaction = this.dataBrokerService.beginTransaction();
            addFlowTransaction.putConfigurationData(instIdFlow, f.build());
            listTransactionStatus.add(addFlowTransaction.commit());
            logger.debug("Commiting flow for {} with config {}", nId, f.build());
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
        addFlowTransaction.putConfigurationData(instIdFlow, flowBody);
        logger.debug("Commiting flow for {} with config {}", nodeId, flowBody);
        return addFlowTransaction.commit();
    }

    private NodeConnectorId nodeConnectorId(InstanceIdentifier<org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node> d, String connectorId) {
        NodeKey nodeKey = d.firstKeyOf(org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node.class, NodeKey.class);
        String stringId = nodeKey.getId().getValue() + ":" + connectorId;
        return new NodeConnectorId(stringId);
    }

    public void programNode(NodeConnector srcNodeConnector, NodeConnector dstNodeConnector, MacAddress srcMac, MacAddress dstMac, PacketReceived notification) {
//        SrcDstPair sdp = new SrcDstPair(srcMac, dstMac, srcNodeConnector, dstNodeConnector);

        FlowBuilder fb = FlowUtils.createEthFlowOutPort(srcNodeConnector, dstNodeConnector, srcMac, dstMac, 10);
        if (notification != null) {
            fb.setBufferId(notification.getBufferId().getValue());
        }

//        ca.insertFlow(fb, sdp);
        writeFlowToNode(srcNodeConnector.getNode(), fb.build());
    }
}
