package org.opendaylight.multiroute.cache;

import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.core.Path;

public class CachedRoute extends Route {

    private final NodeConnector srcNodeCon;

    private final NodeConnector dstNodeCon;

    public CachedRoute(int routeId,
                       Node srcNode,
                       Node dstNode,
                       Path principal,
                       Path backup,
                       NodeConnector srcNodeCon,
                       NodeConnector dstNodeCon) {
        super(routeId, -1, srcNode, dstNode, principal, backup);
        this.srcNodeCon = srcNodeCon;
        this.dstNodeCon = dstNodeCon;
    }

    public NodeConnector getSrcNodeCon() {
        return srcNodeCon;
    }

    public NodeConnector getDstNodeCon() {
        return dstNodeCon;
    }
}
