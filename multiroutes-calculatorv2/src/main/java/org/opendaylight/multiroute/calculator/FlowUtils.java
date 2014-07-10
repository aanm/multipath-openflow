package org.opendaylight.multiroute.calculator;

import com.google.common.collect.ImmutableList;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.core.Path;
import org.opendaylight.multiroute.cache.SrcDstPair;
import static org.opendaylight.multiroute.calculator.Cache.getCache;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.inet.types.rev100924.Uri;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.GroupActionCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.OutputActionCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.group.action._case.GroupActionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.output.action._case.OutputActionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.list.Action;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.list.ActionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.list.ActionKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.FlowModFlags;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.OutputPortValues;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.InstructionsBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.MatchBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.instruction.ApplyActionsCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.instruction.apply.actions._case.ApplyActions;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.instruction.apply.actions._case.ApplyActionsBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.list.Instruction;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.list.InstructionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.BucketId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.GroupId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.GroupTypes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.group.BucketsBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.group.buckets.Bucket;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.group.buckets.BucketBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.group.buckets.BucketKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.groups.GroupBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.groups.GroupKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.ethernet.match.fields.EthernetDestinationBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.ethernet.match.fields.EthernetSourceBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.match.EthernetMatch;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.match.EthernetMatchBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowUtils {

    private static final int FLOW_TIMETOUT = 0;
    private static final AtomicInteger cookieId = new AtomicInteger(1);
    private static final long IN_PORT = 0x0FFF8L;

    private static final Cache db = getCache();

    private static final ConcurrentHashMap<Node, AtomicInteger> nodeCurrentGrpIds = new ConcurrentHashMap<>();
    protected static final Logger logger = LoggerFactory.getLogger(FlowUtils.class);

    /**
     * Creates a new flow builder to match for the given inNodeConnector, srcMac
     * and dstMac and sent that traffic to the given outNodeNonnector. The flow
     * will expire in the given idleTimeout seconds. The flow will have table ID
     * set to 0 and there will be a notification when this flow expires.
     *
     * @param inNodeConnector
     * @param outNodeConnector
     * @param srcMac
     * @param dstMac
     * @param idleTimeout The number of seconds before a flow expires after
     * receiving the last packet that match the specific rule. If set to 0 the
     * rule never expires.
     * @return The flow builder with all the specifications described above.
     */
    public static FlowBuilder
            createEthFlowOutPort(NodeConnector inNodeConnector,
                                 NodeConnector outNodeConnector,
                                 MacAddress srcMac,
                                 MacAddress dstMac,
                                 int idleTimeout) {
        FlowBuilder fb = new FlowBuilder();
        //For now it's table 0
        fb.setTableId((short) 0);
        fb.setIdleTimeout(idleTimeout);
        BigInteger cookieHash = createCookieHash(inNodeConnector, outNodeConnector, null, srcMac, dstMac);
//        fb.setCookie(BigInteger.valueOf(cookieId.getAndIncrement()));
        fb.setCookie(cookieHash);
//        FlowId flowId = new FlowId(String.valueOf(flowIdInc.getAndIncrement()));
//        FlowKey flowKey = new FlowKey(flowId);
        boolean sendFlowRem = idleTimeout != 0;
        fb.setHardTimeout(0);
        //Check if it's a good idea to create an ID based on hash values
        //fb.setId(null);

        //The matching is based on the src eth addr and dst eth addr
        EthernetMatchBuilder emb = new EthernetMatchBuilder();
//        emb.setEthernetSource(new EthernetSourceBuilder()//
//                .setAddress(srcMac)//
//                .build());
        emb.setEthernetDestination(new EthernetDestinationBuilder()//
                .setAddress(dstMac)//
                .build());

        EthernetMatch ethernetMatch = emb.build();

        Uri inport = new Uri(inNodeConnector.getNodeConnectorIDString());
        Uri outport = new Uri(outNodeConnector.getNodeConnectorIDString());

        MatchBuilder match = new MatchBuilder();
        match.setEthernetMatch(ethernetMatch);
        match.setInPort(new NodeConnectorId(inport));

        fb.setMatch(match.build());

        Action outputToPortAction = new ActionBuilder() //
                .setAction(new OutputActionCaseBuilder() //
                        .setOutputAction(new OutputActionBuilder() //
                                .setMaxLength(Integer.valueOf(0xffff)) //
                                .setOutputNodeConnector(outport) //
                                .build()) //
                        .build()) //
                .build();

        // Create an Apply Action
        ApplyActions applyActions = new ApplyActionsBuilder()
                .setAction(ImmutableList.of(outputToPortAction))
                .build();

        // Wrap our Apply Action in an Instruction
        Instruction applyActionsInstruction = new InstructionBuilder() //
                .setInstruction(new ApplyActionsCaseBuilder()//
                        .setApplyActions(applyActions) //
                        .build()) //
                .build();

        fb.setInstructions(new InstructionsBuilder() //
                .setInstruction(ImmutableList.of(applyActionsInstruction)) //
                .build()) //
                .setFlags(new FlowModFlags(false, false, false, false, sendFlowRem));

        return fb;
    }

    /**
     * Creates a flow that match the given inNodeConnector, srcMac and dstMac
     * and sends it to the given groupId.
     *
     * @param inNodeConnector
     * @param grpIdOut
     * @param srcMac
     * @param dstMac
     * @param idleTimeout The number of seconds before a flow expires after
     * receiving the last packet that match the specific rule. If set to 0 the
     * rule never expires.
     * @return The group builder with all the specifications described above.
     */
    public static FlowBuilder
            createEthFlowOutGroup(NodeConnector inNodeConnector,
                                  GroupId grpIdOut,
                                  MacAddress srcMac,
                                  MacAddress dstMac,
                                  int idleTimeout) {
        FlowBuilder fb = new FlowBuilder();
        //For now it's table 0
        fb.setTableId((short) 0);
        fb.setIdleTimeout(idleTimeout);
        BigInteger cookieHash = createCookieHash(inNodeConnector, null, grpIdOut, srcMac, dstMac);
//        fb.setCookie(BigInteger.valueOf(cookieId.getAndIncrement()));
        fb.setCookie(cookieHash);
        boolean sendFlowRem = idleTimeout != 0;
        fb.setHardTimeout(0);
        //Check if it's a good idea to create an ID based on hash values
        //fb.setId(null);

        //The matching is based on the src eth addr and dst eth addr
        EthernetMatchBuilder emb = new EthernetMatchBuilder();
//        emb.setEthernetSource(new EthernetSourceBuilder()//
//                .setAddress(srcMac)//
//                .build());
        emb.setEthernetDestination(new EthernetDestinationBuilder()//
                .setAddress(dstMac)//
                .build());

        EthernetMatch ethernetMatch = emb.build();

        Uri inport = new Uri(inNodeConnector.getNodeConnectorIDString());

        MatchBuilder match = new MatchBuilder();
        match.setEthernetMatch(ethernetMatch);
        match.setInPort(new NodeConnectorId(inport));

        fb.setMatch(match.build());

        Action outputToGroupAction = new ActionBuilder() //
                .setAction(new GroupActionCaseBuilder() //
                        .setGroupAction(new GroupActionBuilder() //
                                .setGroupId(grpIdOut.getValue())
                                .build()) //
                        .build()) //
                .build();

        // Create an Apply Action
        ApplyActions applyActions = new ApplyActionsBuilder()
                .setAction(ImmutableList.of(outputToGroupAction))
                .build();

        // Wrap our Apply Action in an Instruction
        Instruction applyActionsInstruction = new InstructionBuilder() //
                .setInstruction(new ApplyActionsCaseBuilder()//
                        .setApplyActions(applyActions) //
                        .build()) //
                .build();

        fb.setInstructions(new InstructionsBuilder() //
                .setInstruction(ImmutableList.of(applyActionsInstruction)) //
                .build()) //
                .setFlags(new FlowModFlags(false, false, false, false, sendFlowRem));

        return fb;
    }

    /**
     * Creates flows that match the given srcMac and dstMac and sends it to the
     * first group that is in fbgrps.
     *
     * @param srcNodeConnector
     * @param route
     * @param srcMac
     * @param dstMac
     * @param fbgrps List of group builder for a respective node.
     * @return A multi value hash map with all the groupbuilder created for the
     * respective node.
     */
    public static HashMap<Node, List<FlowBuilder>>
            createEthFlowsFromPathWithGroups(NodeConnector srcNodeConnector, Path route, MacAddress srcMac, MacAddress dstMac, HashMap<Node, List<GroupBuilder>> fbgrps) {
        HashMap<Node, List<FlowBuilder>> flows = new HashMap<>();
        List<Edge> edges = route.getEdges();

        HashMapUtils.//
                putMultiValue(flows,//
                              srcNodeConnector.getNode(),//
                              createEthFlowOutGroup(srcNodeConnector,//
                                                    fbgrps.get(srcNodeConnector.getNode()).get(0).getGroupId(),
                                                    srcMac,
                                                    dstMac,
                                                    FLOW_TIMETOUT));
        int i;
        for (i = 0; i < edges.size(); i++) {
            HashMapUtils.//
                    putMultiValue(flows,//
                                  edges.get(i).getHeadNodeConnector().getNode(),//
                                  createEthFlowOutGroup(edges.get(i).getHeadNodeConnector(),//
                                                        fbgrps.get(edges.get(i).getHeadNodeConnector().getNode()).get(0).getGroupId(),
                                                        srcMac,
                                                        dstMac,
                                                        FLOW_TIMETOUT));
        }
        return flows;
    }

    public static HashMap<Node, List<FlowBuilder>>
            createEthFlowsFromPathWithGroupsNew(NodeConnector srcNodeConnector,
                                                NodeConnector dstNodeConnector,
                                                Path route,
                                                MacAddress srcMac,
                                                MacAddress dstMac,
                                                HashMap<Node, List<GroupBuilder>> fbgrps) {
        HashMap<Node, List<FlowBuilder>> flows = new HashMap<>();
        List<Edge> edges = route.getEdges();

        if (containsGroup(fbgrps, srcNodeConnector.getNode())) {
            HashMapUtils.//
                    putMultiValue(flows,//
                                  srcNodeConnector.getNode(),//
                                  createEthFlowOutGroup(srcNodeConnector,//
                                                        fbgrps.get(srcNodeConnector.getNode()).get(0).getGroupId(),
                                                        srcMac,
                                                        dstMac,
                                                        FLOW_TIMETOUT));
        } else {
            HashMapUtils.//
                    putMultiValue(flows,//
                                  srcNodeConnector.getNode(),//
                                  createEthFlowOutPort(srcNodeConnector,//
                                                       edges.get(0).getTailNodeConnector(),
                                                       srcMac,
                                                       dstMac,
                                                       FLOW_TIMETOUT));
        }
        int i;
        for (i = 1; i < edges.size(); i++) {
            if (containsGroup(fbgrps, edges.get(i - 1).getHeadNodeConnector().getNode())) {
                HashMapUtils.//
                        putMultiValue(flows,//
                                      edges.get(i - 1).getHeadNodeConnector().getNode(),//
                                      createEthFlowOutGroup(edges.get(i - 1).getHeadNodeConnector(),//
                                                            fbgrps.get(edges.get(i - 1).getHeadNodeConnector().getNode()).get(0).getGroupId(),
                                                            srcMac,
                                                            dstMac,
                                                            FLOW_TIMETOUT));
            } else {
                HashMapUtils.//
                        putMultiValue(flows,//
                                      edges.get(i - 1).getHeadNodeConnector().getNode(),//
                                      createEthFlowOutPort(edges.get(i - 1).getHeadNodeConnector(),//
                                                           edges.get(i).getTailNodeConnector(),
                                                           srcMac,
                                                           dstMac,
                                                           FLOW_TIMETOUT));
            }
        }
        HashMapUtils.//
                putMultiValue(flows,//
                              dstNodeConnector.getNode(),//
                              createEthFlowOutPort(edges.get(i - 1).getHeadNodeConnector(),//
                                                   dstNodeConnector,
                                                   srcMac,
                                                   dstMac,
                                                   FLOW_TIMETOUT));
        return flows;
    }

    private static boolean containsGroup(HashMap<Node, List<GroupBuilder>> fbgrps, Node node) {
        List<GroupBuilder> groupBuilderList = fbgrps.get(node);
        return groupBuilderList != null && groupBuilderList.get(0) != null;
    }

    /**
     * Returns a new GroupId for the given node.
     *
     * @param node
     * @return The new groupId.
     */
    private static int getNewGroupId(Node node) {
        nodeCurrentGrpIds.putIfAbsent(node, new AtomicInteger());
        return nodeCurrentGrpIds.get(node).getAndIncrement();
    }

    /**
     * Returns the correspondent groupId for the given Uri in and the given Uri
     * out for the respective node. If it doesn't exist it's created a new one.
     *
     * @param node
     * @param in
     * @param out
     * @return the correspondent groupId for the given Uri in and the given Uri
     * out for the respective node.
     */
    private static int getGroupId(Node node, Uri in, Uri out) {
        int groupId = db.getGroupId(node, in, out);
        if (groupId == -1) {
            groupId = getNewGroupId(node);
            db.insertGroupID(node, in, out, groupId);
        }
        return groupId;
    }

    /**
     * Creates a group for the respective node. If the backupOutNodeConnector is
     * set to null the group will have it's backup bucket the inNodeConnector,
     * if it's not null the group will set it's backup bucket the
     * outNodeConnector.
     *
     * @param inNodeConnector The ingress port.
     * @param outNodeConnector The egress port.
     * @param backupOutNodeConnector The backup port, could be null see the
     * description for more details.
     * @return The group builder with a principal bucket and a backup bucket.
     */
    private static GroupBuilder
            createGroupForNode(NodeConnector inNodeConnector, NodeConnector outNodeConnector, NodeConnector backupOutNodeConnector) {

        GroupKey key;
        long groupId;
        GroupBuilder group = new GroupBuilder();
        group.setGroupType(GroupTypes.GroupFf);
        BucketBuilder principalBucket = new BucketBuilder();
        BucketBuilder backupBucket = new BucketBuilder();
        Uri ingressPort;
        Uri principalOutPort;
        Uri backupOutPort;

        principalBucket.setBucketId(new BucketId(1L));
        principalBucket.setKey(new BucketKey(new BucketId(1L)));
        principalOutPort = new Uri(outNodeConnector.getNodeConnectorIDString());
        principalBucket.setAction(createNormalOutputAction(principalOutPort));
        principalBucket.setWatchPort((long) getPortFromUri(principalOutPort));

        backupBucket.setBucketId(new BucketId(2L));
        backupBucket.setKey(new BucketKey(new BucketId(2L)));
        if (backupOutNodeConnector == null) {
            backupOutPort = new Uri(OutputPortValues.INPORT.toString());
            ingressPort = new Uri(inNodeConnector.getNodeConnectorIDString());
            backupBucket.setWatchPort((long) getPortFromUri(ingressPort));
        } else {
            backupOutPort = new Uri(backupOutNodeConnector.getNodeConnectorIDString());
            backupBucket.setWatchPort((long) getPortFromUri(backupOutPort));
        }
        backupBucket.setAction(createNormalOutputAction(backupOutPort));

        BucketsBuilder bucketsBuilder = new BucketsBuilder();
        List<Bucket> bucketList = new ArrayList<>();

        bucketList.add(principalBucket.build());
        bucketList.add(backupBucket.build());

        bucketsBuilder.setBucket(bucketList);

        groupId = getGroupId(inNodeConnector.getNode(), principalOutPort, backupOutPort);

        key = new GroupKey(new GroupId(groupId));
        group.setKey(key);
        group.setGroupId(new GroupId(groupId));
        group.setBarrier(false);

        group.setBuckets(bucketsBuilder.build());

        return group;
    }

    private static String
            getSwitchFromUri(Uri uri) {
        return uri.getValue().substring(0, uri.getValue().lastIndexOf(':'));
    }

    private static long
            getPortFromUri(Uri uri) {
        return Long.parseLong(uri.getValue().substring(uri.getValue().lastIndexOf(':') + 1));
    }

    private static List<Action>
            createNormalOutputAction(Uri outputPort) {
        List<Action> actions = new ArrayList<>();
        ActionBuilder ab = new ActionBuilder();
        OutputActionBuilder output = new OutputActionBuilder();
        output.setMaxLength(0xffff);
        output.setOutputNodeConnector(outputPort);
        ab.setAction(new OutputActionCaseBuilder().setOutputAction(output.build()).build());
        ab.setKey(new ActionKey(1));
        actions.add(ab.build());
        return actions;
    }

    public static HashMap<Node, List<FlowBuilder>> createEthFromPairOrigDst(HashMap<Node, HashSet<SrcDstPair>> pairsOrigDst) {
        HashMap<Node, List<FlowBuilder>> flows = new HashMap<>();
        for (Map.Entry<Node, HashSet<SrcDstPair>> srcDstPairsOfNode : pairsOrigDst.entrySet()) {
            HashSet<SrcDstPair> value = srcDstPairsOfNode.getValue();
            Iterator<SrcDstPair> iterator = value.iterator();
            while (iterator.hasNext()) {
                SrcDstPair next = iterator.next();
                FlowBuilder createEthFlowOutPort;
                createEthFlowOutPort = createEthFlowOutPort(next.getSrcNodeConnector(),
                                                            next.getDstNodeConnector(),
                                                            next.getSrcEth(),
                                                            next.getDstEth(),
                                                            0);
                HashMapUtils.//
                        putMultiValue(flows,
                                      srcDstPairsOfNode.getKey(),
                                      createEthFlowOutPort);
            }
        }
        return flows;
    }

    public static HashMap<Node, List<GroupBuilder>>
            createGroupsFromPairsOrigDst(Path primaryPath,
                                         HashMap<Node, HashSet<SrcDstPair>> primaryPairsOrigDst,
                                         HashMap<Node, HashSet<SrcDstPair>> pairsOrigDst,
                                         HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> backuprouteWithgroups) {
        HashMap<Node, List<GroupBuilder>> groups = new HashMap<>();
        for (Node node : primaryPath.getNodes()) {
            HashSet<SrcDstPair> get = primaryPairsOrigDst.get(node);
            Iterator<SrcDstPair> iterator = get.iterator();
            if (iterator.hasNext()) {
                SrcDstPair tm = iterator.next();
                SrcDstPair fallBacked = isFallBacked(tm.getSrcNodeConnector(),
                                                     tm.getSrcEth(),
                                                     tm.getDstEth(),
                                                     pairsOrigDst);
                if (fallBacked != null) {
                    NodeConnector fallBackPort = null;
                    if (!fallBacked.getDstNodeConnector().equals(tm.getSrcNodeConnector())) {
                        fallBackPort = fallBacked.getDstNodeConnector();
                    }
                    HashMapUtils.//
                            putMultiValue(groups,//
                                          tm.getSrcNodeConnector().getNode(),//
                                          createGroupForNode(tm.getSrcNodeConnector(),//
                                                             tm.getDstNodeConnector(),
                                                             fallBackPort));
                    pairsOrigDst.get(node).remove(fallBacked);
                    for (Iterator<SrcDstPair> it = pairsOrigDst.get(node).iterator(); it.hasNext();) {
                        SrcDstPair sdp = it.next();
                        if (sdp.getDstNodeConnector().equals(tm.getDstNodeConnector()) && sdp.getDstEth().equals(tm.getDstEth())) {
                            if (!fallBacked.getDstNodeConnector().equals(sdp.getSrcNodeConnector())) {
                                fallBackPort = fallBacked.getDstNodeConnector();
                            }
                            GroupBuilder createGroupForNode = createGroupForNode(sdp.getSrcNodeConnector(),//
                                                                                 sdp.getDstNodeConnector(),
                                                                                 fallBackPort);
                            HashMapUtils.//
                                    putMultiValue(backuprouteWithgroups,//
                                                  sdp.getSrcNodeConnector().getNode(),//
                                                  createEthFlowOutGroup(sdp.getSrcNodeConnector(),//
                                                                        createGroupForNode.getGroupId(),
                                                                        sdp.getSrcEth(),
                                                                        sdp.getDstEth(),
                                                                        FLOW_TIMETOUT));

                            HashMapUtils.//
                                    putMultiValue(groups,//
                                                  sdp.getSrcNodeConnector().getNode(),//
                                                  createGroupForNode);

                            it.remove();
                        }
                    }
                }
            }
        }
        return groups;
    }

    private static SrcDstPair isFallBacked(NodeConnector srcNodeConnector, MacAddress srcEth, MacAddress dstEth, HashMap<Node, HashSet<SrcDstPair>> pairsOrigDst) {
        HashSet<SrcDstPair> srcDstPairSet = pairsOrigDst.get(srcNodeConnector.getNode());
        if (srcDstPairSet != null) {
            Iterator<SrcDstPair> itSrcDstPairSet = srcDstPairSet.iterator();
            while (itSrcDstPairSet.hasNext()) {
                SrcDstPair next = itSrcDstPairSet.next();
                if (next.getSrcNodeConnector().equals(srcNodeConnector)
                        //    && next.getSrcEth().equals(srcEth)
                        && next.getDstEth().equals(dstEth)) {
                    return next;
                }
            }
        }
        return null;
    }

    private static BigInteger createCookieHash(NodeConnector inNodeConnector,
                                               NodeConnector outNodeConnector,
                                               GroupId groupId,
                                               MacAddress srcMac,
                                               MacAddress dstMac) {
        final long prime = 31;
        long hash = 1;
//        result = prime * result + ((inNodeConnector == null) ? 0 : inNodeConnector.hashCode());
//        result = prime * result + ((srcMac == null) ? 0 : srcMac.hashCode());
//        result = prime * result + ((dstMac == null) ? 0 : dstMac.hashCode());
//
//        result = prime * result + ((outNodeConnector == null) ? 0 : outNodeConnector.hashCode());
//
//        result = prime * result + ((groupId == null) ? 0 : ((long) groupId.hashCode() << 32));

//        int hash = 3;
        hash = 73 * hash + Objects.hashCode(inNodeConnector);
        hash = 73 * hash + Objects.hashCode(outNodeConnector);
//        hash = 73 * hash + Objects.hashCode(srcMac);
        hash = 73 * hash + Objects.hashCode(dstMac);
        hash = 73 * hash + Objects.hashCode(groupId);

        BigInteger b = BigInteger.valueOf(hash);
        if (b.signum() < 0) {
            b = b.add(TWO_64);
        }
        return b;
    }

    private static final BigInteger TWO_64 = BigInteger.ONE.shiftLeft(64);
}
