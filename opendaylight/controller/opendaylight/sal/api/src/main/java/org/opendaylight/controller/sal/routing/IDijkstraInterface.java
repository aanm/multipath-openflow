/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.opendaylight.controller.sal.routing;

import java.util.HashMap;
import java.util.List;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.core.Path;

/**
 *
 * @author aanm
 */
public interface IDijkstraInterface extends IRouting {

    HashMap<NodeConnector, List<Path>> getRouteWithoutSingleEdges(final NodeConnector srcNodeConnector, final Node dst, final List<Edge> edges);

    Path getRouteWithoutAllEdges(final Node src, final Node dst, final List<Edge> edges);

    Path getRouteWithoutNode(final Node src, final Node dst, final Node middle);

    Path getRouteWithoutEdge(final Node src, final Node dst, final Edge edge);

    void setListenRoutingUpdatesPrivate(final IListenRoutingUpdatesWrapper iListenRouteingUpdates);
}
