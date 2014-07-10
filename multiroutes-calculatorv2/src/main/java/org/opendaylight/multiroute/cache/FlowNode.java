package org.opendaylight.multiroute.cache;

import org.opendaylight.controller.sal.core.Node;

public class FlowNode {

    private long flowId;
    private Node node;

    public FlowNode(long flowId, Node node) {
        this.flowId = flowId;
        this.node = node;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 67 * hash + (int) (this.flowId ^ (this.flowId >>> 32));
        hash = 67 * hash + ((node == null) ? 0 : node.hashCode());
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final FlowNode other = (FlowNode) obj;
        if (this.flowId != other.flowId) {
            return false;
        }
        if (!this.node.equals(other.node)) {
            return false;
        }
        return true;
    }

    public long getFlowId() {
        return flowId;
    }

    public Node getNode() {
        return node;
    }

}
