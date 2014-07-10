package org.opendaylight.multiroute.cache;

import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.Path;

public class Route {

    private Node srcNode;
    private Node dstNode;
    private Path principal;
    private Path backup;
    private final int routeId;
    private final int routeIdReverse;

    public Route(int routeId, int routeIdReverse, Node srcNode, Node dstNode, Path principal, Path backup) {
        this.routeId = routeId;
        this.routeIdReverse = routeIdReverse;
        this.srcNode = srcNode;
        this.dstNode = dstNode;
        this.principal = principal;
        this.backup = backup;
    }

    public int getRouteId() {
        return routeId;
    }

    public int getRouteIdReverse() {
        return routeIdReverse;
    }

    public Node getSrcNode() {
        return srcNode;
    }

    public void setSrcNode(Node srcNode) {
        this.srcNode = srcNode;
    }

    public Node getDstNode() {
        return dstNode;
    }

    public void setDstNode(Node dstNode) {
        this.dstNode = dstNode;
    }

    public Path getPrincipal() {
        return principal;
    }

    public void setPrincipal(Path principal) {
        this.principal = principal;
    }

    public Path getBackup() {
        return backup;
    }

    public void setBackup(Path backup) {
        this.backup = backup;
    }

    public Route reverse() {
        Route toReturn;
        if (routeIdReverse != -1) {
            Path pPathReverse = principal == null ? null : principal.reverse();
            Path bPathReverse = backup == null ? null : backup.reverse();
            toReturn = new Route(routeIdReverse,
                                 routeId,
                                 dstNode,
                                 srcNode,
                                 pPathReverse,
                                 bPathReverse);
        } else {
            toReturn = null;
        }
        return toReturn;
    }

    public String toString() {
        String pPathString = principal == null ? "null" : principal.toString();
        String bPathString = backup == null ? "null" : backup.toString();
        return "ROUTE ID = [" + routeId + "] PPATH = " + pPathString + "] BPATH = [" + bPathString + "]";
    }
}
