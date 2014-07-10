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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((srcNodeConnector == null) ? 0 : srcNodeConnector.hashCode());
        result = prime * result + ((dstNodeConnector == null) ? 0 : dstNodeConnector.hashCode());
        result = prime * result + ((srcEth == null) ? 0 : srcEth.hashCode());
        result = prime * result + ((dstEth == null) ? 0 : dstEth.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final SrcDstPair other = (SrcDstPair) obj;
        if (!this.srcNodeConnector.equals(other.srcNodeConnector)) {
            return false;
        }
        if (!this.dstNodeConnector.equals(other.dstNodeConnector)) {
            return false;
        }
        if (!this.srcEth.equals(other.srcEth)) {
            return false;
        }
        if (!this.dstEth.equals(other.dstEth)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "SrcDstPair{" + "srcNodeConnector=" + srcNodeConnector + ", dstNodeConnector=" + dstNodeConnector + ", srcEth=" + srcEth + ", dstEth=" + dstEth + '}';
    }
}
