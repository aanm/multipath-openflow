/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.opendaylight.controller.sal.routing;

import java.util.List;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.topology.TopoEdgeUpdate;

/**
 *
 * @author aanm
 */
public interface IListenRoutingUpdatesWrapper extends IListenRoutingUpdates {

    /**
     * Method invoked when the recalculation of the all shortest path tree is
     * done
     *
     * @param changeEdges
     */
    public void recalculateDone(List<Edge> changeEdges);
    public void recalculateDoneTopo(List<TopoEdgeUpdate> changeEdges);

}
