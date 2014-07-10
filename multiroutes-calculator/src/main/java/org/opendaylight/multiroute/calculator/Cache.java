package org.opendaylight.multiroute.calculator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.h2.Driver;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.core.Path;
import org.opendaylight.multiroute.cache.CachedRoute;
import org.opendaylight.multiroute.cache.Route;
import org.opendaylight.multiroute.cache.SrcDstPair;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.inet.types.rev100924.Uri;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.Flow;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cache {

    private static final String dbName = "jdbc:h2:mem:db1;MULTI_THREADED=true";
    private static final String dbUser = "root";
    private static final String dbPass = "toor";

    private static final Cache cache = new Cache();
    protected static final Logger logger = LoggerFactory.getLogger(Cache.class);

    public static Cache getCache() {
        return cache;
    }

    private Connection conn = null;
    private final ConcurrentHashMap<Integer, Node> nodes;
    private final ConcurrentHashMap<Integer, MacAddress> macAddresses;
    private final ConcurrentHashMap<Long, Flow> flows;
    private final ConcurrentHashMap<Integer, Path> paths;
    private final ConcurrentHashMap<Integer, NodeConnector> nodeConnectors;
    private final ConcurrentHashMap<Integer, Edge> edges;

    public Cache() {
        this.nodes = new ConcurrentHashMap<>();
        this.macAddresses = new ConcurrentHashMap<>();
        this.flows = new ConcurrentHashMap<>();
        this.paths = new ConcurrentHashMap<>();
        this.edges = new ConcurrentHashMap<>();
        this.nodeConnectors = new ConcurrentHashMap<>();
        try {
            Driver.load();
            conn = DriverManager.getConnection(dbName, dbUser, dbPass);
            conn.setAutoCommit(true);
        } catch (SQLException ex) {
            logger.error(ex.getMessage());
        }
        try {
            createSchema();
        } catch (SQLException ex) {
        }
    }

    public int getGroupId(Node node, Uri portIn, Uri portOut) {
        long nodeId = node.hashCode();
        long portInId = portIn.hashCode();
        long portOutId = portOut.hashCode();
        int groupId = -1;
        try {
            Statement statement = conn.createStatement();
            String insertFlowSql = "SELECT GROUPID "
                    + "FROM GROUPS "
                    + "WHERE NODE=" + nodeId + " AND "
                    + "PORTIN=" + portInId + " AND "
                    + "PORTOUT=" + portOutId + ";";
            try {
                ResultSet resultSet = statement.executeQuery(insertFlowSql);
                if (resultSet.next()) {
                    try {
                        groupId = resultSet.getInt("GROUPID");
                    } catch (SQLException ex) {
                        logger.error(ex.getLocalizedMessage());
                    }
                }
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
        return groupId;
    }

    public void insertGroupID(Node node, Uri portIn, Uri portOut, int groupId) {
        long nodeId = node.hashCode();
        long portInId = portIn.hashCode();
        long portOutId = portOut.hashCode();
        try {
            Statement statement = conn.createStatement();
            String insertFlowSql = "MERGE INTO GROUPS("
                    + "NODE, PORTIN, PORTOUT, GROUPID) VALUES( "
                    + nodeId + ", "
                    + portInId + ", "
                    + portOutId + ", "
                    + groupId + ")";
            try {
                statement.executeUpdate(insertFlowSql);
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }

        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
    }

    public List<Route> getAllRoutesWithoutEdges(List<Route> routesFromEdges, List<Edge> changeEdges) {
        StringBuilder sb = new StringBuilder("SELECT DISTINCT ");
        sb.append("ROUTES.SRCNODE,ROUTES.DSTNODE,ROUTES.PPATH,ROUTES.BPATH, ROUTES.ROUTEID FROM EDGES, ");
        sb.append("ROUTES WHERE (ROUTES.PPATH=EDGES.PATH OR ROUTES.BPATH=EDGES.PATH) AND (EDGE<>");
        for (Edge e : changeEdges) {
            sb.append(e.hashCode());
            sb.append(" AND EDGE<>");
            sb.append(e.reverse().hashCode());
            sb.append(" AND EDGE<>");
        }
        sb.delete(sb.length() - " AND EDGE<>".length(), sb.length());
        sb.append(");");

        List<Route> routes = new ArrayList<>();
        int routeId, srcNodeId, dstNodeId, pPathId, bPathId;
        try {
            Statement statement = conn.createStatement();
            String insertFlowSql = sb.toString();
            try {
                ResultSet resultSet = statement.executeQuery(insertFlowSql);
                while (resultSet.next()) {
                    try {
                        routeId = resultSet.getInt("ROUTES.ROUTEID");
                        srcNodeId = resultSet.getInt("ROUTES.SRCNODE");
                        dstNodeId = resultSet.getInt("ROUTES.DSTNODE");
                        pPathId = resultSet.getInt("ROUTES.PPATH");
                        bPathId = resultSet.getInt("ROUTES.BPATH");
                        routes.add(new Route(routeId, -1, nodes.get(srcNodeId), nodes.get(dstNodeId),
                                             paths.get(pPathId), paths.get(bPathId)));
                    } catch (SQLException ex) {
                        logger.error(ex.getLocalizedMessage());
                    }
                }
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
        return routes;
    }

    public List<Route> getAllRoutesFromEdges(List<Edge> changeEdges) {
        StringBuilder sb = new StringBuilder("SELECT DISTINCT ");
        sb.append("ROUTES.SRCNODE,ROUTES.DSTNODE,ROUTES.PPATH,ROUTES.BPATH, ROUTES.ROUTEID FROM EDGES, ");
        sb.append("ROUTES WHERE (ROUTES.PPATH=EDGES.PATH OR ROUTES.BPATH=EDGES.PATH) AND (EDGE=");
        for (Edge e : changeEdges) {
            sb.append(e.hashCode());
            sb.append(" OR EDGE=");
        }
        sb.delete(sb.length() - " OR EDGE=".length(), sb.length());
        sb.append(");");
        List<Route> routes = new ArrayList<>();
        int routeId, srcNodeId, dstNodeId, pPathId, bPathId;
        try {
            Statement statement = conn.createStatement();
            String insertFlowSql = sb.toString();
            try {
                ResultSet resultSet = statement.executeQuery(insertFlowSql);
                while (resultSet.next()) {
                    try {
                        routeId = resultSet.getInt("ROUTES.ROUTEID");
                        srcNodeId = resultSet.getInt("ROUTES.SRCNODE");
                        dstNodeId = resultSet.getInt("ROUTES.DSTNODE");
                        pPathId = resultSet.getInt("ROUTES.PPATH");
                        bPathId = resultSet.getInt("ROUTES.BPATH");
                        int reverseRouteId = getRoute(dstNodeId, srcNodeId);
                        routes.add(new Route(routeId, reverseRouteId, nodes.get(srcNodeId), nodes.get(dstNodeId),
                                             paths.get(pPathId), paths.get(bPathId)));
                    } catch (SQLException ex) {
                        logger.error(ex.getLocalizedMessage());
                    }
                }
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
        return routes;
    }

    private int getRoute(int srcNodeId, int dstNodeId) {
        int reverseRouteId = -1;
        String routeSql = "SELECT ROUTES.ROUTEID FROM ROUTES"
                + " WHERE ROUTES.SRCNODE="
                + srcNodeId
                + " AND ROUTES.DSTNODE="
                + dstNodeId + ";";
        try {
            Statement statement = conn.createStatement();
            try {
                ResultSet routeResultSet = statement.executeQuery(routeSql);
                if (routeResultSet.next()) {
                    try {
                        if (routeResultSet.next()) {
                            reverseRouteId = routeResultSet.getInt("ROUTES.ROUTEID");
                        }
                    } catch (SQLException ex) {
                        logger.error(ex.getLocalizedMessage());
                    }
                }
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
        return reverseRouteId;
    }

    public List<Route> getAllRoutes() {
        StringBuilder sb = new StringBuilder("SELECT DISTINCT ");
        sb.append("ROUTES.SRCNODE,ROUTES.DSTNODE,ROUTES.PPATH,ROUTES.BPATH, ROUTES.ROUTEID FROM ROUTES;");

        List<Route> routes = new ArrayList<>();
        int routeId, srcNodeId, dstNodeId, pPathId, bPathId;
        try {
            Statement statement = conn.createStatement();
            String insertFlowSql = sb.toString();
            try {
                ResultSet resultSet = statement.executeQuery(insertFlowSql);
                while (resultSet.next()) {
                    try {
                        routeId = resultSet.getInt("ROUTES.ROUTEID");
                        srcNodeId = resultSet.getInt("ROUTES.SRCNODE");
                        dstNodeId = resultSet.getInt("ROUTES.DSTNODE");
                        pPathId = resultSet.getInt("ROUTES.PPATH");
                        bPathId = resultSet.getInt("ROUTES.BPATH");
                        routes.add(new Route(routeId, -1, nodes.get(srcNodeId), nodes.get(dstNodeId),
                                             paths.get(pPathId), paths.get(bPathId)));
                    } catch (SQLException ex) {
                        logger.error(ex.getLocalizedMessage());
                    }
                }
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
        return routes;
    }

    public List<SrcDstPair> getSrcDstPairs(Route r) {
        StringBuilder sb = new StringBuilder("SELECT DISTINCT ");
        sb.append("PAIRORIGDST.SRCMAC, PAIRORIGDST.DSTMAC, PAIRORIGDST.SRCNODECON, PAIRORIGDST.DSTNODECON ");
        sb.append("FROM PAIRORIGDST ");
        sb.append("WHERE ROUTEID = ");
        sb.append(r.getRouteId());
        sb.append(";");
        List<SrcDstPair> sdps = new ArrayList<>();
        int srcEthAddId, dstEthAddId, srcNodeConId, dstNodeConId;
        try {
            Statement statement = conn.createStatement();
            String insertFlowSql = sb.toString();
            try {
                ResultSet resultSet = statement.executeQuery(insertFlowSql);
                while (resultSet.next()) {
                    try {
                        srcEthAddId = resultSet.getInt("PAIRORIGDST.SRCMAC");
                        dstEthAddId = resultSet.getInt("PAIRORIGDST.DSTMAC");
                        srcNodeConId = resultSet.getInt("PAIRORIGDST.SRCNODECON");
                        dstNodeConId = resultSet.getInt("PAIRORIGDST.DSTNODECON");
                        sdps.add(new SrcDstPair(macAddresses.get(srcEthAddId),
                                                macAddresses.get(dstEthAddId),
                                                nodeConnectors.get(srcNodeConId),
                                                nodeConnectors.get(dstNodeConId)));
                    } catch (SQLException ex) {
                        logger.error(ex.getLocalizedMessage());
                    }
                }
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
        return sdps;
    }

    public void saveNewPaths(Route r, Path newP, Path backupPath) {
        if (r == null) {
            return;
        }
        int primaryRouteId = newP.hashCode();
        paths.put(primaryRouteId, newP);
        int backupRouteId = 0;
        if (backupPath != null) {
            backupRouteId = backupPath.hashCode();
            paths.put(backupRouteId, backupPath);
        }
        Statement statement = null;
        try {
            try {
                statement = conn.createStatement();
                String insertFlowSql = "MERGE INTO ROUTES("
                        + "ROUTEID, SRCNODE, DSTNODE, PPATH, BPATH) VALUES( "
                        + r.getRouteId() + ", "
                        + r.getSrcNode().hashCode() + ", "
                        + r.getDstNode().hashCode() + ", "
                        + primaryRouteId + ", "
                        + backupRouteId + ")";
                statement.executeUpdate(insertFlowSql);
            } catch (SQLException ex) {
                logger.error(ex.getLocalizedMessage());
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
        if (r.getPrincipal() != null) {
            int oldPrimaryRouteId = r.getPrincipal().hashCode();
            if (primaryRouteId != oldPrimaryRouteId && backupRouteId != oldPrimaryRouteId) {
                Path oldPPath = paths.remove(oldPrimaryRouteId);
                removeEdgesPath(oldPPath);
            }
        }
        if (r.getBackup() != null) {
            int oldBackupRouteId = r.getBackup().hashCode();
            if (backupRouteId != oldBackupRouteId && primaryRouteId != oldBackupRouteId) {
                Path oldBPath = paths.remove(oldBackupRouteId);
                removeEdgesPath(oldBPath);
            }
        }
        r.setPrincipal(newP);
        r.setBackup(backupPath);
    }

    public int insertRoute(Node sourceNode, Node destinationNode, Path primaryRoute, Path backupRoute) {
        int sourceNodeId = sourceNode.hashCode();
        int destinationNodeId = destinationNode.hashCode();
        int primaryRouteId = primaryRoute.hashCode();
        int backupRouteId = 0;
        if (backupRoute != null) {
            backupRouteId = backupRoute.hashCode();
            paths.put(backupRouteId, backupRoute);
        }
        nodes.put(sourceNodeId, sourceNode);
        nodes.put(destinationNodeId, destinationNode);
        paths.put(primaryRouteId, primaryRoute);
        int routeId = -1;
        Statement statement = null;
        try {
            StringBuilder sb = new StringBuilder("SELECT DISTINCT ");
            sb.append("ROUTES.ROUTEID ");
            sb.append("FROM ROUTES ");
            sb.append("WHERE SRCNODE = ");
            sb.append(sourceNodeId);
            sb.append(" AND DSTNODE = ");
            sb.append(destinationNodeId);
            sb.append(" AND PPATH = ");
            sb.append(primaryRouteId);
            sb.append(" AND BPATH = ");
            sb.append(backupRouteId);
            sb.append(";");

            try {
                statement = conn.createStatement();
                String insertFlowSql = "INSERT INTO ROUTES("
                        + "SRCNODE, DSTNODE, PPATH, BPATH) VALUES( "
                        + sourceNodeId + ", "
                        + destinationNodeId + ", "
                        + primaryRouteId + ", "
                        + backupRouteId + ")";

                ResultSet resultSet = statement.executeQuery(sb.toString());
                if (resultSet.next()) {
                    routeId = resultSet.getInt("ROUTES.ROUTEID");
                } else {
                    statement.executeUpdate(insertFlowSql);
                    statement.executeQuery(sb.toString());
                    resultSet = statement.getResultSet();
                    if (resultSet.next()) {
                        routeId = resultSet.getInt("ROUTES.ROUTEID");
                    }
                }
            } catch (SQLException ex) {
                logger.error(ex.getLocalizedMessage());
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
        return routeId;
    }

    public void savePairOrig(MacAddress srcMac, MacAddress dstMac, NodeConnector srcNodeConnector, NodeConnector dstNodeConnector, int routeIdSrcDst) {
        int srcMacId = srcMac.hashCode();
        int dstMacId = dstMac.hashCode();
        int srcNodeConnectorId = srcNodeConnector.hashCode();
        int dstNodeConnectorId = dstNodeConnector.hashCode();
        macAddresses.put(srcMacId, srcMac);
        macAddresses.put(dstMacId, dstMac);
        nodeConnectors.put(srcNodeConnectorId, srcNodeConnector);
        nodeConnectors.put(dstNodeConnectorId, dstNodeConnector);
        try {
            Statement statement = conn.createStatement();
            String insertFlowSql = "MERGE INTO PAIRORIGDST("
                    + "SRCMAC, DSTMAC, SRCNODECON, DSTNODECON, ROUTEID) VALUES( "
                    + srcMacId + ", "
                    + dstMacId + ", "
                    + srcNodeConnectorId + ", "
                    + dstNodeConnectorId + ", "
                    + routeIdSrcDst + ")";

            try {
                statement.executeUpdate(insertFlowSql);
            } catch (SQLException ex) {
                logger.error(ex.getLocalizedMessage());
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
    }

    public void removeEdgesPath(Path path) {
        if (path == null) {
            return;
        }
        int pathId = path.hashCode();
        try {
            Statement statement = conn.createStatement();
            try {
                String insertFlowSql = "DELETE FROM EDGES WHERE "
                        + "PATH = " + pathId;
                statement.executeUpdate(insertFlowSql);
            } catch (SQLException ex) {
                logger.error(ex.getLocalizedMessage());
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
    }

    public void insertEdgesPath(Path path) {
        int pathId = path.hashCode();
        paths.put(pathId, path);
        try {
            Statement statement = conn.createStatement();
            try {
                for (Edge edge : path.getEdges()) {
                    int edgeId = edge.hashCode();
                    edges.put(edgeId, edge);
                    String insertFlowSql = "MERGE INTO EDGES("
                            + "EDGE, PATH) VALUES( "
                            + edgeId + ", "
                            + pathId + ")";
                    statement.executeUpdate(insertFlowSql);
                }
            } catch (SQLException ex) {
                logger.error(ex.getLocalizedMessage());
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }

        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
    }

    public CachedRoute getRoute(Node sourceNode, MacAddress srcMac, MacAddress dstMac) {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append("ROUTES.SRCNODE,ROUTES.DSTNODE,ROUTES.PPATH,ROUTES.BPATH, ROUTES.ROUTEID, ");
        sb.append("PAIRORIGDST.SRCNODECON, PAIRORIGDST.DSTNODECON FROM ROUTES, ");
        sb.append("PAIRORIGDST WHERE (ROUTES.ROUTEID=PAIRORIGDST.ROUTEID) AND ");
        sb.append("PAIRORIGDST.SRCMAC=").append(srcMac.hashCode()).append(" AND ");
        sb.append("PAIRORIGDST.DSTMAC=").append(dstMac.hashCode()).append(" AND ");
        sb.append("ROUTES.SRCNODE=").append(sourceNode.hashCode()).append(";");
        CachedRoute route = null;
        int routeId, srcNodeId, dstNodeId, pPathId, bPathId, srcNodeConId, dstNodeConId;
        try {
            Statement statement = conn.createStatement();
            String insertFlowSql = sb.toString();
            try {
                ResultSet resultSet = statement.executeQuery(insertFlowSql);
                if (resultSet.next()) {
                    try {
                        routeId = resultSet.getInt("ROUTES.ROUTEID");
                        srcNodeId = resultSet.getInt("ROUTES.SRCNODE");
                        dstNodeId = resultSet.getInt("ROUTES.DSTNODE");
                        pPathId = resultSet.getInt("ROUTES.PPATH");
                        bPathId = resultSet.getInt("ROUTES.BPATH");
                        srcNodeConId = resultSet.getInt("PAIRORIGDST.SRCNODECON");
                        dstNodeConId = resultSet.getInt("PAIRORIGDST.DSTNODECON");
                        route = new CachedRoute(routeId,
                                                nodes.get(srcNodeId),
                                                nodes.get(dstNodeId),
                                                paths.get(pPathId),
                                                paths.get(bPathId),
                                                nodeConnectors.get(srcNodeConId),
                                                nodeConnectors.get(dstNodeConId));
                    } catch (SQLException ex) {
                        logger.error(ex.getLocalizedMessage());
                    }
                }
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
        return route;
    }

    public HashMap<Node, List<FlowBuilder>>
            getEthFlowsFromPath(Path backupRoute,
                                MacAddress srcMac,
                                MacAddress dstMac,
                                FlowType type) {
        int srcMacId = srcMac.hashCode();
        int dstMacId = dstMac.hashCode();
        StringBuilder sb = new StringBuilder("SELECT DISTINCT ");
        sb.append(type.getValue()).append(".FLOW ");
        sb.append("FROM ").append(type.getValue());
        sb.append(" WHERE SRCMAC = ");
        sb.append(srcMacId);
        sb.append(" AND DSTMAC = ");
        sb.append(dstMacId);
        sb.append(" AND NODE = ");
        String searchFlow = sb.toString();
        HashMap<Node, List<FlowBuilder>> flowsToReturn = new HashMap<>();
        List<Edge> edgesList = backupRoute.getEdges();

        try {
            Statement statement = conn.createStatement();
            try {
                switch (type) {
                    case BACKUP:
                        flowsToReturn = getEthFlowsBackupFromPath(statement, searchFlow, edgesList);
                        break;
                    case EXPERIMENTAL:
                        flowsToReturn = getEthFlowsExpFromPath(statement, searchFlow, edgesList);
                        break;
                    case NORMAL:
                        flowsToReturn = getEthFlowsNormalFromPath(statement, searchFlow, edgesList);
                        break;
                }
            } catch (SQLException ex) {
                logger.error(ex.getLocalizedMessage());
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
        return flowsToReturn;
    }

    public FlowBuilder getFlowBuilder(Node sourceNode, MacAddress srcMac, MacAddress dstMac) {
        StringBuilder sb = new StringBuilder("SELECT DISTINCT ");
        sb.append("NORMALFLOWS.FLOW ");
        sb.append("FROM NORMALFLOWS ");
        sb.append("WHERE NODE = ");
        sb.append(sourceNode.hashCode());
        sb.append("AND SRCMAC = ");
        sb.append(srcMac.hashCode());
        sb.append("AND DSTMAC = ");
        sb.append(dstMac.hashCode());
        sb.append(";");
        FlowBuilder toReturn = null;
        try {
            Statement statement = conn.createStatement();
            String insertFlowSql = sb.toString();
            try {
                ResultSet resultSet = statement.executeQuery(insertFlowSql);
                if (resultSet.next()) {
                    try {
                        long flowId = resultSet.getLong("NORMALFLOWS.FLOW");
                        Flow flow = flows.get(flowId);
                        if (flow != null) {
                            toReturn = new FlowBuilder(flow);
                        }
                    } catch (SQLException ex) {
                        logger.error(ex.getLocalizedMessage());
                    }
                }
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
        return toReturn;
    }

    public void insertFlows(HashMap<Node, List<FlowBuilder>> fbs, MacAddress srcMac, MacAddress dstMac, FlowType type) {
        int srcMacId = srcMac.hashCode();
        int dstMacId = dstMac.hashCode();
        macAddresses.put(srcMacId, srcMac);
        macAddresses.put(dstMacId, dstMac);
        try {
            for (Map.Entry<Node, List<FlowBuilder>> nodesFlows : fbs.entrySet()) {
                Node node = nodesFlows.getKey();
                int nodeId = node.hashCode();
                nodes.put(nodeId, node);
                for (FlowBuilder fb : nodesFlows.getValue()) {
                    long flowBuilderId = fb.getCookie().longValue();
                    flows.put(flowBuilderId, fb.build());
                    Statement statement = conn.createStatement();
                    String insertFlowSql = "MERGE INTO " + type.getValue() + "("
                            + "NODE, SRCMAC, DSTMAC, FLOW) VALUES( "
                            + nodeId + ", "
                            + srcMacId + ", "
                            + dstMacId + ", "
                            + flowBuilderId + ")";
                    try {
                        statement.executeUpdate(insertFlowSql);
                    } finally {
                        if (statement != null) {
                            statement.close();
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getLocalizedMessage());
        }
    }

    private HashMap<Node, List<FlowBuilder>> getEthFlowsNormalFromPath(Statement statement, String searchFlow, List<Edge> edgesList) throws SQLException {
        HashMap<Node, List<FlowBuilder>> flowsToReturn = new HashMap<>();
        Node node = edgesList.get(0).getTailNodeConnector().getNode();
        ResultSet resultSet = statement.executeQuery(searchFlow + node.hashCode() + ";");
        if (resultSet.next()) {
            long flowId = resultSet.getLong(FlowType.NORMAL.getValue() + ".FLOW");
            Flow flow = flows.get(flowId);
            if (flow != null) {
                HashMapUtils.//
                        putMultiValue(flowsToReturn,//
                                      node,//
                                      new FlowBuilder(flow));
            } else {
                return null;
            }
        } else {
            return null;
        }
        for (Edge edge : edgesList) {
            node = edge.getHeadNodeConnector().getNode();
            resultSet = statement.executeQuery(searchFlow + node.hashCode() + ";");
            if (resultSet.next()) {
                long flowId = resultSet.getLong(FlowType.NORMAL.getValue() + ".FLOW");
                Flow flow = flows.get(flowId);
                if (flow != null) {
                    HashMapUtils.//
                            putMultiValue(flowsToReturn,//
                                          node,//
                                          new FlowBuilder(flow));
                } else {
                    flowsToReturn = null;
                    break;
                }
            } else {
                flowsToReturn = null;
                break;
            }
        }
        return flowsToReturn;
    }

    private HashMap<Node, List<FlowBuilder>> getEthFlowsExpFromPath(Statement statement, String searchFlow, List<Edge> edgesList) throws SQLException {
        HashMap<Node, List<FlowBuilder>> flowsToReturn = new HashMap<>();
        Node node = edgesList.get(0).getTailNodeConnector().getNode();
        ResultSet resultSet = statement.executeQuery(searchFlow + node.hashCode() + ";");
        if (resultSet.next()) {
            long flowId = resultSet.getLong(FlowType.EXPERIMENTAL.getValue() + ".FLOW");
            Flow flow = flows.get(flowId);
            if (flow != null) {
                HashMapUtils.//
                        putMultiValue(flowsToReturn,//
                                      node,//
                                      new FlowBuilder(flow));
            } else {
                return null;
            }
        } else {
            return null;
        }
        for (int i = 0; i < edgesList.size() - 1; i++) {
            node = edgesList.get(i).getHeadNodeConnector().getNode();
            resultSet = statement.executeQuery(searchFlow + node.hashCode() + ";");
            if (resultSet.next()) {
                long flowId = resultSet.getLong(FlowType.EXPERIMENTAL.getValue() + ".FLOW");
                Flow flow = flows.get(flowId);
                if (flow != null) {
                    HashMapUtils.//
                            putMultiValue(flowsToReturn,//
                                          node,//
                                          new FlowBuilder(flow));
                } else {
                    flowsToReturn = null;
                    break;
                }
            } else {
                flowsToReturn = null;
                break;
            }
        }
        return flowsToReturn;
    }

    private HashMap<Node, List<FlowBuilder>> getEthFlowsBackupFromPath(Statement statement, String searchFlow, List<Edge> edgesList) throws SQLException {
        HashMap<Node, List<FlowBuilder>> flowsToReturn = new HashMap<>();
        for (Edge edge : edgesList) {
            Node node = edge.getHeadNodeConnector().getNode();
            ResultSet resultSet = statement.executeQuery(searchFlow + node.hashCode() + ";");
            if (resultSet.next()) {
                long flowId = resultSet.getLong(FlowType.BACKUP.getValue() + ".FLOW");
                Flow flow = flows.get(flowId);
                if (flow != null) {
                    HashMapUtils.//
                            putMultiValue(flowsToReturn,//
                                          node,//
                                          new FlowBuilder(flow));
                } else {
                    flowsToReturn = null;
                    break;
                }
            } else {
                flowsToReturn = null;
                break;
            }
        }
        return flowsToReturn;
    }

    private void createSchema() throws SQLException {
        Statement statement = null;
        String normalFlows = "CREATE TABLE NORMALFLOWS("
                + "NODE INT NOT NULL, "
                + "SRCMAC INT NOT NULL, "
                + "DSTMAC INT NOT NULL, "
                + "FLOW BIGINT, "
                + "PRIMARY KEY (NODE,SRCMAC,DSTMAC))";
        String expFlows = "CREATE TABLE EXPFLOWS("
                + "NODE INT NOT NULL, "
                + "SRCMAC INT NOT NULL, "
                + "DSTMAC INT NOT NULL, "
                + "FLOW BIGINT, "
                + "PRIMARY KEY (NODE,SRCMAC,DSTMAC))";
        String backupFlows = "CREATE TABLE BACKUPFLOWS("
                + "NODE INT NOT NULL, "
                + "SRCMAC INT NOT NULL, "
                + "DSTMAC INT NOT NULL, "
                + "FLOW BIGINT, "
                + "PRIMARY KEY (NODE,SRCMAC,DSTMAC))";
        String edgesTable = "CREATE TABLE EDGES("
                + "EDGE INT NOT NULL, "
                + "PATH INT NOT NULL, "
                + "PRIMARY KEY (EDGE,PATH))";
        String routes = "CREATE TABLE ROUTES("
                + "ROUTEID INT NOT NULL AUTO_INCREMENT, "
                + "SRCNODE INT NOT NULL, "
                + "DSTNODE INT NOT NULL, "
                + "PPATH INT, "
                + "BPATH INT, "
                + "PRIMARY KEY (ROUTEID))";
        String pairOrigDest = "CREATE TABLE PAIRORIGDST("
                + "SRCMAC INT NOT NULL, "
                + "DSTMAC INT NOT NULL, "
                + "SRCNODECON INT NOT NULL, "
                + "DSTNODECON INT NOT NULL, "
                + "ROUTEID INT NOT NULL, "
                + "PRIMARY KEY(SRCMAC, DSTMAC, SRCNODECON, DSTNODECON, ROUTEID), "
                + "FOREIGN KEY(ROUTEID) REFERENCES ROUTES(ROUTEID))";
        String groupsTable = "CREATE TABLE GROUPS("
                + "NODE BIGINT NOT NULL, "
                + "PORTIN BIGINT NOT NULL, "
                + "PORTOUT BIGINT NOT NULL, "
                + "GROUPID INT NOT NULL, "
                + "PRIMARY KEY (NODE,PORTIN,PORTOUT) "
                + ")";
        try {
            statement = conn.createStatement();
            statement.executeUpdate(normalFlows);
            statement.executeUpdate(expFlows);
            statement.executeUpdate(backupFlows);
            statement.executeUpdate(edgesTable);
            statement.executeUpdate(routes);
            statement.executeUpdate(pairOrigDest);
            statement.executeUpdate(groupsTable);
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    public enum FlowType {

        NORMAL("NORMALFLOWS"), BACKUP("BACKUPFLOWS"), EXPERIMENTAL("EXPFLOWS");
        private final String value;

        private FlowType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

}
