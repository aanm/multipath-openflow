package org.opendaylight.multiroute.calculator;

import com.google.common.collect.ImmutableList;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.xml.bind.DatatypeConverter;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.core.Path;
import static org.opendaylight.multiroute.calculator.Cache.getCache;
import org.opendaylight.multiroute.experimentermessages.ExperimenterNXActionRegLoad;
import org.opendaylight.multiroute.experimentermessages.ExperimenterNXLearn;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.inet.types.rev100924.Uri;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.ExperimenterActionCase;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.ExperimenterActionCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.GroupActionCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.OutputActionCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.experimenter.action._case.ExperimenterActionBuilder;
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

    private static final AtomicInteger cookieId = new AtomicInteger(1);
    private static final long IN_PORT = 0x0FFF8L;

    private static final Cache db = getCache();

    private static final ConcurrentHashMap<Node, AtomicInteger> nodeCurrentGrpIds = new ConcurrentHashMap<>();
    protected static final Logger logger = LoggerFactory.getLogger(FlowUtils.class);

    /**
     * Creates a new (experimenter) flow builder to match for the given
     * outNodeConnector, srcMac and dstMac. If a match is matched the packet is
     * sent to the newOutNodeConnector. At the same time the new newOutPort is
     * load to a register in the switch. That value it will be used to set
     * output for a flow in the switch that match inNodeConnector, srcMac and
     * dstMac.
     *
     * @param inNodeConnector
     * @param outNodeConnector
     * @param newOutPort
     * @param newOutNodeConnector
     * @param srcMac
     * @param dstMac
     * @param idleTimeout The number of seconds before a flow expires after
     * receiving the last packet that match the specific rule. If set to 0 the
     * rule never expires.
     * @return The flow builder with all the specifications described above.
     */
    public static FlowBuilder
            createExpFlowOutPort(NodeConnector inNodeConnector,
                                 NodeConnector outNodeConnector,
                                 long newOutPort,
                                 Uri newOutNodeConnector,
                                 MacAddress srcMac,
                                 MacAddress dstMac,
                                 BigInteger cookieOfFlowToMatch,
                                 int idleTimeout) {

        byte[] srcMacByte = DatatypeConverter.parseHexBinary(srcMac.getValue().replaceAll(":", ""));
        byte[] dstMacByte = DatatypeConverter.parseHexBinary(dstMac.getValue().replaceAll(":", ""));
        /**
         * 'in_port=outNodeConnector,dl_src=srcMac,dl_dst=dstMac
         * actions=output:inNodeConnector, load:0xfff8->NXM_NX_REG0[0..15],
         * learn(table=0, in_port=inNodeConnector, dl_src=srcMac, dl_dst=dstMac,
         * output:NXM_NX_REG0[0..15])'
         */
//        FlowModInputBuilder f = new FlowModInputBuilder().setCommand(FlowModCommand.OFPFCDELETE);
//        FlowModInput build = f.build();

        FlowBuilder fb = new FlowBuilder();
        //For now it's table 0
        fb.setTableId((short) 0);
        fb.setIdleTimeout(idleTimeout);
        fb.setCookie(BigInteger.valueOf(cookieId.getAndIncrement()));
        boolean sendFlowRem = idleTimeout != 0;
        fb.setHardTimeout(0);
        //Check if it's a good idea to create an ID based on hash values
        //fb.setId(null);

        //The matching is based on the src eth addr and dst eth addr
        EthernetMatchBuilder emb = new EthernetMatchBuilder();
        emb.setEthernetSource(new EthernetSourceBuilder()//
                .setAddress(srcMac)//
                .build());
        emb.setEthernetDestination(new EthernetDestinationBuilder()//
                .setAddress(dstMac)//
                .build());

        EthernetMatch ethernetMatch = emb.build();

        Uri inport = new Uri(inNodeConnector.getNodeConnectorIDString());
        Uri outport = new Uri(outNodeConnector.getNodeConnectorIDString());

        MatchBuilder match = new MatchBuilder();
        match.setEthernetMatch(ethernetMatch);
        match.setInPort(new NodeConnectorId(outport));

        fb.setMatch(match.build());

        Action outputToPortAction = new ActionBuilder() //
                .setAction(new OutputActionCaseBuilder() //
                        .setOutputAction(new OutputActionBuilder() //
                                .setMaxLength(Integer.valueOf(0xffff)) //
                                .setOutputNodeConnector(newOutNodeConnector) //
                                .build()) //
                        .build()) //
                .setOrder(0)
                .build();

        ExperimenterNXActionRegLoad enarl = new ExperimenterNXActionRegLoad(0, false);
        enarl.loadValueToRegister(newOutPort, 0, 15);
        ExperimenterActionBuilder eabRegLoad = enarl.build();

        ExperimenterActionCase eacRegLoad = new ExperimenterActionCaseBuilder().setExperimenterAction(eabRegLoad.build()).build();

        ActionBuilder actionRegLoad = new ActionBuilder();
        actionRegLoad.setAction(eacRegLoad);
        actionRegLoad.setOrder(1);

        ExperimenterNXLearn enxl = new ExperimenterNXLearn();
        enxl.setEthSrc(srcMacByte);
        enxl.setEthDst(dstMacByte);
        enxl.setTable_id(0);
        enxl.setPortIn((int) getPortFromUri(inport));
        enxl.setOutputFromRegister(0, 0, 15);
        enxl.setCookie(cookieOfFlowToMatch.longValue());
        ExperimenterActionBuilder eabLearn = enxl.build();

        ExperimenterActionCase eacLearn = new ExperimenterActionCaseBuilder().setExperimenterAction(eabLearn.build()).build();

        ActionBuilder actionLearn = new ActionBuilder();
        actionLearn.setAction(eacLearn);
        actionLearn.setOrder(2);

        List<Action> actions = new ArrayList<>();

        actions.add(outputToPortAction);
        actions.add(actionRegLoad.build());
        actions.add(actionLearn.build());

        ApplyActionsCaseBuilder aaBldr = new ApplyActionsCaseBuilder();
        aaBldr.setApplyActions(new ApplyActionsBuilder().setAction(actions).build());

        InstructionBuilder instructionBldr = new InstructionBuilder();
        instructionBldr.setInstruction(aaBldr.build());

        List<Instruction> instructions = Collections.singletonList(instructionBldr.build());
        InstructionsBuilder instructionsBldr = new InstructionsBuilder();
        instructionsBldr.setInstruction(instructions);

        // Wrap our Apply Action in an Instruction
        Instruction applyActionsInstruction = instructionBldr.build();

        fb.setInstructions(new InstructionsBuilder() //
                .setInstruction(ImmutableList.of(applyActionsInstruction)) //
                .build()) //
                .setFlags(new FlowModFlags(false, false, false, false, sendFlowRem));

        return fb;
    }

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
        fb.setCookie(BigInteger.valueOf(cookieId.getAndIncrement()));
//        FlowId flowId = new FlowId(String.valueOf(flowIdInc.getAndIncrement()));
//        FlowKey flowKey = new FlowKey(flowId);
        boolean sendFlowRem = idleTimeout != 0;
        fb.setHardTimeout(0);
        //Check if it's a good idea to create an ID based on hash values
        //fb.setId(null);

        //The matching is based on the src eth addr and dst eth addr
        EthernetMatchBuilder emb = new EthernetMatchBuilder();
        emb.setEthernetSource(new EthernetSourceBuilder()//
                .setAddress(srcMac)//
                .build());
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
        fb.setCookie(BigInteger.valueOf(cookieId.getAndIncrement()));
        boolean sendFlowRem = idleTimeout != 0;
        fb.setHardTimeout(0);
        //Check if it's a good idea to create an ID based on hash values
        //fb.setId(null);

        //The matching is based on the src eth addr and dst eth addr
        EthernetMatchBuilder emb = new EthernetMatchBuilder();
        emb.setEthernetSource(new EthernetSourceBuilder()//
                .setAddress(srcMac)//
                .build());
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

    public static HashMap<Node, List<FlowBuilder>>
            createEthFlowsFromPath(NodeConnector srcNodeConnector,
                                   NodeConnector dstNodeConnector,
                                   Path route,
                                   MacAddress srcMac,
                                   MacAddress dstMac) {
        HashMap<Node, List<FlowBuilder>> flows = new HashMap<>();
        List<Edge> edges = route.getEdges();

        HashMapUtils.//
                putMultiValue(flows,//
                              srcNodeConnector.getNode(),//
                              createEthFlowOutPort(srcNodeConnector,//
                                                   edges.get(0).getTailNodeConnector(),
                                                   srcMac,
                                                   dstMac,
                                                   10));
        int i;
        for (i = 1; i < edges.size(); i++) {
            HashMapUtils.//
                    putMultiValue(flows,//
                                  edges.get(i - 1).getHeadNodeConnector().getNode(),//
                                  createEthFlowOutPort(edges.get(i - 1).getHeadNodeConnector(),//
                                                       edges.get(i).getTailNodeConnector(),
                                                       srcMac,
                                                       dstMac,
                                                       10));
        }
        HashMapUtils.//
                putMultiValue(flows,//
                              edges.get(i - 1).getHeadNodeConnector().getNode(),//
                              createEthFlowOutPort(edges.get(i - 1).getHeadNodeConnector(),//
                                                   dstNodeConnector,
                                                   srcMac,
                                                   dstMac,
                                                   10));
        return flows;
    }

    /**
     * In the backup path we don't program anything at all for the
     * srcNodeConnector. We leave that for primary route. We only set a flow for
     * the destination node because in the backup node because, instead of the
     * source node, the destination node sends all the receiving traffic to the
     * same port.
     *
     * @param dstNodeConnector
     * @param route
     * @param srcMac
     * @param dstMac
     * @return A multi value hash map with all the flowbuilder created for the
     * respective node.
     */
    public static HashMap<Node, List<FlowBuilder>>
            createEthFlowsFromBackupPath(NodeConnector dstNodeConnector,
                                         Path route,
                                         MacAddress srcMac,
                                         MacAddress dstMac) {
        HashMap<Node, List<FlowBuilder>> flows = new HashMap<>();
        List<Edge> edges = route.getEdges();

        int i;
        for (i = 1; i < edges.size(); i++) {
            HashMapUtils.//
                    putMultiValue(flows,//
                                  edges.get(i - 1).getHeadNodeConnector().getNode(),//
                                  createEthFlowOutPort(edges.get(i - 1).getHeadNodeConnector(),//
                                                       edges.get(i).getTailNodeConnector(),//
                                                       srcMac,//
                                                       dstMac,//
                                                       0));
        }
        HashMapUtils.//
                putMultiValue(flows,//
                              edges.get(i - 1).getHeadNodeConnector().getNode(),//
                              createEthFlowOutPort(edges.get(i - 1).getHeadNodeConnector(),//
                                                   dstNodeConnector,//
                                                   srcMac,//
                                                   dstMac,//
                                                   0));
        return flows;
    }

    /**
     * Creates GroupBuilders for the given route. All nodes have the principal
     * port their normal output port and the ingress port their backup port.
     *
     * @param srcNodeConnector
     * @param dstNodeConnector
     * @param route
     * @param backupRoute
     * @return A list of group builders for every respective node.
     */
    public static HashMap<Node, List<GroupBuilder>>
            createPeripheralWayBackGroupsFromPath(NodeConnector srcNodeConnector, NodeConnector dstNodeConnector, Path route, Path backupRoute) {
        HashMap<Node, List<GroupBuilder>> groups = new HashMap<>();
        List<Edge> edges = route.getEdges();

        HashMapUtils.//
                putMultiValue(groups,//
                              srcNodeConnector.getNode(),//
                              createGroupForNode(srcNodeConnector,//
                                                 edges.get(0).getTailNodeConnector(),
                                                 backupRoute.getEdges().get(0).getTailNodeConnector()));
        /**
         * I'm not sure if it's really necessary to create groups for the last
         * node.
         */
        HashMapUtils.//
                putMultiValue(groups,//
                              dstNodeConnector.getNode(),//
                              createGroupForNode(edges.get(edges.size() - 1).getHeadNodeConnector(),
                                                 dstNodeConnector,
                                                 null));
        return groups;
    }

    /**
     * Creates GroupBuilders for the given route. All nodes have the principal
     * port their normal output port and the ingress port their backup port.
     *
     * @param srcNodeConnector
     * @param dstNodeConnector
     * @param route
     * @param backupRoute
     * @return A list of group builders for every respective node.
     */
    public static HashMap<Node, List<GroupBuilder>>
            createCoreWayBackGroupsFromPath(NodeConnector dstNodeConnector, Path route, Path backupRoute) {
        HashMap<Node, List<GroupBuilder>> groups = new HashMap<>();
        List<Edge> edges = route.getEdges();

        int i;
        for (i = 1; i < edges.size(); i++) {
            HashMapUtils.//
                    putMultiValue(groups,//
                                  edges.get(i - 1).getHeadNodeConnector().getNode(),//
                                  createGroupForNode(edges.get(i - 1).getHeadNodeConnector(),
                                                     edges.get(i).getTailNodeConnector(),
                                                     null));
        }
        return groups;
    }

    /**
     * Creates GroupBuilders for the given route. All nodes except the first
     * node have the principal port their normal output port and the ingress
     * port their backup port. The first node have their principal port the
     * normal port of the principal route and it's backup port it's the first
     * port of the backup route.
     *
     * @param srcNodeConnector
     * @param dstNodeConnector
     * @param route
     * @param backupRoute
     * @return A list of group builders for every respective node.
     */
    public static HashMap<Node, List<GroupBuilder>>
            createWayBackGroupsFromPath(NodeConnector srcNodeConnector, NodeConnector dstNodeConnector, Path route, Path backupRoute) {
        HashMap<Node, List<GroupBuilder>> groups = new HashMap<>();
        List<Edge> edges = route.getEdges();

        HashMapUtils.//
                putMultiValue(groups,//
                              srcNodeConnector.getNode(),//
                              createGroupForNode(srcNodeConnector,//
                                                 edges.get(0).getTailNodeConnector(),
                                                 backupRoute.getEdges().get(0).getTailNodeConnector()));
        int i;
        for (i = 1; i < edges.size(); i++) {
            HashMapUtils.//
                    putMultiValue(groups,//
                                  edges.get(i - 1).getHeadNodeConnector().getNode(),//
                                  createGroupForNode(edges.get(i - 1).getHeadNodeConnector(),
                                                     edges.get(i).getTailNodeConnector(),
                                                     null));
        }
        /**
         * I'm not sure if it's really necessary to create groups for the last
         * node.
         */
        HashMapUtils.//
                putMultiValue(groups,//
                              dstNodeConnector.getNode(),//
                              createGroupForNode(edges.get(i - 1).getHeadNodeConnector(),
                                                 dstNodeConnector,
                                                 null));
        return groups;
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
    public static HashMap<Node, List<FlowBuilder>> createCoreEthFlowsFromPath(Path route, MacAddress srcMac, MacAddress dstMac) {
        HashMap<Node, List<FlowBuilder>> flows = new HashMap<>();
        List<Edge> edges = route.getEdges();

        int i;
        //It's edges.size() -1 because the last headnodeconnector's edge is a peripheral node
        for (i = 0; i < edges.size() - 1; i++) {
            HashMapUtils.//
                    putMultiValue(flows,//
                                  edges.get(i).getHeadNodeConnector().getNode(),//
                                  createEthFlowOutPort(edges.get(i).getHeadNodeConnector(),//
                                                       edges.get(i + 1).getTailNodeConnector(),//
                                                       srcMac,//
                                                       dstMac,//
                                                       10));
        }

        return flows;
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
    public static HashMap<Node, List<FlowBuilder>> createCoreEthFlowsFromPathWithGroups(Path route, MacAddress srcMac, MacAddress dstMac, HashMap<Node, List<GroupBuilder>> fbgrps) {
        HashMap<Node, List<FlowBuilder>> flows = new HashMap<>();
        List<Edge> edges = route.getEdges();

        int i;
        //It's edges.size() -1 because the last headnodeconnector's edge is a peripheral node
        for (i = 0; i < edges.size() - 1; i++) {
            HashMapUtils.//
                    putMultiValue(flows,//
                                  edges.get(i).getHeadNodeConnector().getNode(),//
                                  createEthFlowOutGroup(edges.get(i).getHeadNodeConnector(),//
                                                        fbgrps.get(edges.get(i).getHeadNodeConnector().getNode()).get(0).getGroupId(),
                                                        srcMac,
                                                        dstMac,
                                                        10));
        }
        return flows;
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
            createPeripheralEthFlowsFromPath(NodeConnector srcNodeConnector,
                                             NodeConnector dstNodeConnector,
                                             Path route,
                                             MacAddress srcMac,
                                             MacAddress dstMac) {
        HashMap<Node, List<FlowBuilder>> flows = new HashMap<>();
        List<Edge> edges = route.getEdges();

        HashMapUtils.//
                putMultiValue(flows,//
                              srcNodeConnector.getNode(),//
                              createEthFlowOutPort(srcNodeConnector,
                                                   edges.get(0).getTailNodeConnector(),
                                                   srcMac,
                                                   dstMac,
                                                   10));
        HashMapUtils.//
                putMultiValue(flows,//
                              edges.get(edges.size() - 1).getHeadNodeConnector().getNode(),//
                              createEthFlowOutPort(edges.get(edges.size() - 1).getHeadNodeConnector(),
                                                   dstNodeConnector,
                                                   srcMac,
                                                   dstMac,
                                                   10));
        return flows;
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
            createPeripheralEthFlowsFromPathWithGroups(NodeConnector srcNodeConnector, Path route, MacAddress srcMac, MacAddress dstMac, HashMap<Node, List<GroupBuilder>> fbgrps) {
        HashMap<Node, List<FlowBuilder>> flows = new HashMap<>();
        List<Edge> edges = route.getEdges();

        HashMapUtils.//
                putMultiValue(flows,//
                              srcNodeConnector.getNode(),//
                              createEthFlowOutGroup(srcNodeConnector,//
                                                    fbgrps.get(srcNodeConnector.getNode()).get(0).getGroupId(),
                                                    srcMac,
                                                    dstMac,
                                                    10));
        HashMapUtils.//
                putMultiValue(flows,//
                              edges.get(edges.size() - 1).getHeadNodeConnector().getNode(),//
                              createEthFlowOutGroup(edges.get(edges.size() - 1).getHeadNodeConnector(),//
                                                    fbgrps.get(edges.get(edges.size() - 1).getHeadNodeConnector().getNode()).get(0).getGroupId(),
                                                    srcMac,
                                                    dstMac,
                                                    10));
        return flows;
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
                                                    10));
        int i;
        for (i = 0; i < edges.size(); i++) {
            HashMapUtils.//
                    putMultiValue(flows,//
                                  edges.get(i).getHeadNodeConnector().getNode(),//
                                  createEthFlowOutGroup(edges.get(i).getHeadNodeConnector(),//
                                                        fbgrps.get(edges.get(i).getHeadNodeConnector().getNode()).get(0).getGroupId(),
                                                        srcMac,
                                                        dstMac,
                                                        10));
        }
        return flows;
    }

    /**
     * Creates the experimental flows builders for every node, except the last
     * node, in the given route. Every node, except the last one will have their
     * new output nodeConnector the in_port for that flow.
     *
     * @param srcNodeConnector
     * @param route
     * @param backupRoute
     * @param srcMac
     * @param dstMac
     * @return A multi value hash map with all the flowbuilder created for the
     * respective node.
     */
    public static HashMap<Node, List<FlowBuilder>>
            createCoreExpFlowsFromPath(NodeConnector srcNodeConnector,
                                       Path route,
                                       MacAddress srcMac,
                                       MacAddress dstMac,
                                       HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsWithGroups) {
        HashMap<Node, List<FlowBuilder>> flows = new HashMap<>();
        List<Edge> edges = route.getEdges();
        /**
         * TODO, check if it's possible to set a timeout for the learning rule.
         */
        int i;
        for (i = 1; i < edges.size(); i++) {
            HashMapUtils.//
                    putMultiValue(flows,//
                                  edges.get(i - 1).getHeadNodeConnector().getNode(),//
                                  createExpFlowOutPort(edges.get(i - 1).getHeadNodeConnector(),//
                                                       edges.get(i).getTailNodeConnector(),//
                                                       IN_PORT,
                                                       new Uri(edges.get(i - 1).getHeadNodeConnector().getNodeConnectorIDString()),
                                                       srcMac,//
                                                       dstMac,//
                                                       flowsWithGroups.get(edges.get(i - 1).getHeadNodeConnector().getNode()).get(0).getCookie(),
                                                       0));
        }
        return flows;
    }

    public static HashMap<Node, List<FlowBuilder>>
            createPeripheralExpFlowsFromPath(NodeConnector srcNodeConnector,
                                             Path route,
                                             Path backupRoute,
                                             MacAddress srcMac,
                                             MacAddress dstMac,
                                             HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> flowsWithGroups) {
        HashMap<Node, List<FlowBuilder>> flows = new HashMap<>();
        List<Edge> edges = route.getEdges();
        Uri backupOutportForSrcNode = new Uri(backupRoute.getEdges().get(0).getTailNodeConnector().getNodeConnectorIDString());
        /**
         * TODO, check if it's possible to set a timeout for the learning rule.
         */
        HashMapUtils.//
                putMultiValue(flows,//
                              srcNodeConnector.getNode(),//
                              createExpFlowOutPort(srcNodeConnector,//
                                                   edges.get(0).getTailNodeConnector(),//
                                                   getPortFromUri(backupOutportForSrcNode),//
                                                   backupOutportForSrcNode,
                                                   srcMac,//
                                                   dstMac,//
                                                   flowsWithGroups.get(srcNodeConnector.getNode()).get(0).getCookie(),
                                                   0));
        return flows;
    }

    /**
     * Creates the experimental flows builders for every node, except the last
     * node, in the given route. The first node will have it's new output
     * nodeConnector the first nodeConnector of the backup route. Every
     * remaining node, except the last one will have their new output
     * nodeConnector the in_port for that flow.
     *
     * @param srcNodeConnector
     * @param route
     * @param backupRoute
     * @param srcMac
     * @param dstMac
     * @return A multi value hash map with all the flowbuilder created for the
     * respective node.
     */
    public static HashMap<Node, List<FlowBuilder>>
            createExpFlowsFromPath(NodeConnector srcNodeConnector,
                                   Path route,
                                   Path backupRoute,
                                   MacAddress srcMac,
                                   MacAddress dstMac,
                                   HashMap<org.opendaylight.controller.sal.core.Node, List<FlowBuilder>> normalFlows) {
        HashMap<Node, List<FlowBuilder>> flows = new HashMap<>();
        List<Edge> edges = route.getEdges();
        Uri backupOutportForSrcNode = new Uri(backupRoute.getEdges().get(0).getTailNodeConnector().getNodeConnectorIDString());
        /**
         * TODO, check if it's possible to set a timeout for the learning rule.
         */
        HashMapUtils.//
                putMultiValue(flows,//
                              srcNodeConnector.getNode(),//
                              createExpFlowOutPort(srcNodeConnector,//
                                                   edges.get(0).getTailNodeConnector(),//
                                                   getPortFromUri(backupOutportForSrcNode),//
                                                   backupOutportForSrcNode,
                                                   srcMac,//
                                                   dstMac,//
                                                   normalFlows.get(srcNodeConnector.getNode()).get(0).getCookie(),
                                                   0));
        int i;
        for (i = 1; i < edges.size(); i++) {
            HashMapUtils.//
                    putMultiValue(flows,//
                                  edges.get(i - 1).getHeadNodeConnector().getNode(),//
                                  createExpFlowOutPort(edges.get(i - 1).getHeadNodeConnector(),//
                                                       edges.get(i).getTailNodeConnector(),//
                                                       IN_PORT,
                                                       new Uri(edges.get(i - 1).getHeadNodeConnector().getNodeConnectorIDString()),
                                                       srcMac,//
                                                       dstMac,//
                                                       normalFlows.get(edges.get(i - 1).getHeadNodeConnector().getNode()).get(0).getCookie(),
                                                       0));
        }
        return flows;
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
    private static int
            getGroupId(Node node, Uri in, Uri out) {
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
}
