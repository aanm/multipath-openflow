package org.opendaylight.multiroute.calculator;

import org.opendaylight.controller.sal.compatibility.NodeMapping;
import org.opendaylight.controller.sal.core.ConstructionException;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowCapableNode;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.Table;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.TableKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.Flow;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnector;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnectorKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.Nodes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.NodeKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

public class InstanceIdentifierUtils {

    /**
     * Creates an Instance Identifier (path) for node with specified id
     *
     * @param nodeId
     * @return
     */
    public static InstanceIdentifier<Node> createNodePath(NodeId nodeId) {
        return InstanceIdentifier.builder(Nodes.class) //
                .child(Node.class, new NodeKey(nodeId)) //
                .build();
    }

    /**
     * Shorten's node child path to node path.
     *
     * @param nodeChild child of node, from which we want node path.
     * @return
     */
    public static InstanceIdentifier<Node> getNodePath(InstanceIdentifier<?> nodeChild) {
        return nodeChild.firstIdentifierOf(Node.class);
    }

    /**
     * Creates a table path by appending table specific location to node path
     *
     * @param nodePath
     * @param tableKey
     * @return
     */
    public static InstanceIdentifier<Table> createTablePath(InstanceIdentifier<Node> nodePath, TableKey tableKey) {
        return InstanceIdentifier.builder(nodePath)
                .augmentation(FlowCapableNode.class)
                .child(Table.class, tableKey)
                .build();
    }

    /**
     * Creates a path for particular flow, by appending flow-specific
     * information to table path.
     *
     * @param flowId
     * @param tablePathArg
     * @return path to flow
     */
    public static InstanceIdentifier<Flow> createFlowPath(InstanceIdentifier<Table> table, FlowKey flowKey) {
        return InstanceIdentifier.builder(table)
                .child(Flow.class, flowKey)
                .build();
    }

    /**
     * Extract table id from table path.
     *
     * @param tablePath
     * @return
     */
    public static Short getTableId(InstanceIdentifier<Table> tablePath) {
        return tablePath.firstKeyOf(Table.class, TableKey.class).getId();
    }

    /**
     * Extracts NodeConnectorKey from node connector path.
     *
     * @param nodeConnectorPath The instance identifier for the node connector's
     * path.
     * @return The NodeConnectorKey found.
     */
    public static NodeConnectorKey getNodeConnectorKey(InstanceIdentifier<?> nodeConnectorPath) {
        return nodeConnectorPath.firstKeyOf(NodeConnector.class, NodeConnectorKey.class);
    }

    /**
     * Extracts the nodeId from a given NodeConnectorId.
     *
     * @param nci The NodeConnectorId to extract the nodeId.
     * @return the extracted nodeId.
     */
    public static String getNodeIdFromNodeConnectorKey(NodeConnectorId nci) {
        return nci.getValue().substring(0, nci.getValue().lastIndexOf(':'));
    }

    //
    public static InstanceIdentifier<NodeConnector> createNodeConnectorPath(InstanceIdentifier<Node> nodeKey, NodeConnectorKey nodeConnectorKey) {
        return InstanceIdentifier.builder(nodeKey) //
                .child(NodeConnector.class, nodeConnectorKey) //
                .build();
    }

    /**
     * Creates a NodeConnector from the given nodeConnectorRef instance
     * identifier.
     *
     * @param IngressII The nodeConnectorRef instance identifier.
     * @return The node connector for the given nodeConnectorRef instance
     * identifier.
     * @throws ConstructionException if occurred a problem while creating the
     * source node or the source node connector.
     */
    public static org.opendaylight.controller.sal.core.NodeConnector
            getNodeConnector(InstanceIdentifier<?> IngressII) throws ConstructionException {

        //We need to get the ingressKey in order to get the NodeConnectorId
        NodeConnectorKey ingressKey = getNodeConnectorKey(IngressII);

        NodeConnectorId nodeConId = ingressKey.getId();

        String sourceNodeId = getNodeIdFromNodeConnectorKey(nodeConId);
        org.opendaylight.controller.sal.core.Node sourceNode;

        //We need to get the nodes' types some how
        sourceNode = new org.opendaylight.controller.sal.core.Node(NodeMapping.MD_SAL_TYPE, sourceNodeId);

        org.opendaylight.controller.sal.core.NodeConnector srcNodeConnector;

        srcNodeConnector = new org.opendaylight.controller.sal.core.NodeConnector(NodeMapping.MD_SAL_TYPE, nodeConId.getValue(), sourceNode);

        return srcNodeConnector;
    }
}
