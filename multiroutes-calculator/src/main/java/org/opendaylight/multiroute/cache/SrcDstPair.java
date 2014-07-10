package org.opendaylight.multiroute.cache;

import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.controller.sal.core.NodeConnector;

public class SrcDstPair {

    private final NodeConnector srcNodeConnector;
    private final NodeConnector dstNodeConnector;
    private final MacAddress srcEth;
    private final MacAddress dstEth;

    public SrcDstPair(MacAddress srcEth, MacAddress dstEth, NodeConnector srcNodeConnector, NodeConnector dstNodeConnector) {
        this.srcNodeConnector = srcNodeConnector;
        this.dstNodeConnector = dstNodeConnector;
        this.srcEth = srcEth;
        this.dstEth = dstEth;
    }

    public NodeConnector getSrcNodeConnector() {
        return srcNodeConnector;
    }

    public NodeConnector getDstNodeConnector() {
        return dstNodeConnector;
    }

    public MacAddress getSrcEth() {
        return srcEth;
    }

    public MacAddress getDstEth() {
        return dstEth;
    }

}
