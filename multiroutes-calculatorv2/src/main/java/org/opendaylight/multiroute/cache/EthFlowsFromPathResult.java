package org.opendaylight.multiroute.cache;

import java.util.ArrayList;
import java.util.List;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowBuilder;

public class EthFlowsFromPathResult {

    private List<FlowBuilder> lFbs;

    private NodeConnector srcNodeConnector;

    public EthFlowsFromPathResult(List<FlowBuilder> lFbs, NodeConnector srcNodeConnector) {
        this.lFbs = lFbs;
        this.srcNodeConnector = srcNodeConnector;
    }

    public EthFlowsFromPathResult() {
        this.lFbs = new ArrayList<>();
        this.srcNodeConnector = null;
    }

    public void setSrcNodeConnector(NodeConnector srcNodeConnector) {
        this.srcNodeConnector = srcNodeConnector;
    }

    public List<FlowBuilder> getlFbs() {
        return lFbs;
    }

    public NodeConnector getSrcNodeConnector() {
        return srcNodeConnector;
    }
}
