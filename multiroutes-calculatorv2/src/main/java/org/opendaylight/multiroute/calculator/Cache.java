package org.opendaylight.multiroute.calculator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.h2.Driver;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.core.Path;
import org.opendaylight.multiroute.cache.EthFlowsFromPathResult;
import org.opendaylight.multiroute.cache.FlowNode;
import org.opendaylight.multiroute.cache.SrcDstPair;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.inet.types.rev100924.Uri;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.Flow;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.Match;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.GroupId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.group.types.rev131018.groups.Group;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeId;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
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
    private final ConcurrentHashMap<Long, FlowBuilder> flows;
    private final ConcurrentHashMap<Long, FlowNode> flowNodes;
    private final ConcurrentHashMap<Integer, Edge> edges;
    private final ConcurrentHashMap<SrcDstPair, Path> paths;
    private final ConcurrentHashMap<Integer, SrcDstPair> srcDstPairs;
    private final ConcurrentHashMap<Long, InstanceIdentifier<Flow>> iifs;
    private final ConcurrentHashMap<Long, InstanceIdentifier<Group>> iig;
    private final ConcurrentHashMap<Integer, Long> cookies;

    public Cache() {
        this.flows = new ConcurrentHashMap<>();
        this.edges = new ConcurrentHashMap<>();
        this.srcDstPairs = new ConcurrentHashMap<>();
        this.flowNodes = new ConcurrentHashMap<>();
        this.iifs = new ConcurrentHashMap<>();
        this.paths = new ConcurrentHashMap<>();
        this.iig = new ConcurrentHashMap<>();
        this.cookies = new ConcurrentHashMap<>();
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

    public HashSet<SrcDstPair> getSrcDstPairFrom(List<Edge> changeEdges) {
        StringBuilder sb = new StringBuilder("SELECT DISTINCT SRCDSTPAIRID ");
        sb.append("FROM EDGES ");
        sb.append("WHERE EDGE=");
        for (Edge e : changeEdges) {
            sb.append(e.hashCode());
            sb.append(" OR EDGE=");
        }
        sb.delete(sb.length() - " OR EDGE=".length(), sb.length());
        sb.append(";");
        HashSet<SrcDstPair> srcDstPairsToReturn = new HashSet<>();
        int srcDstPairId;
        try {
            Statement statement = conn.createStatement();
            String insertFlowSql = sb.toString();
            try {
                ResultSet resultSet = statement.executeQuery(insertFlowSql);
                while (resultSet.next()) {
                    try {
                        srcDstPairId = resultSet.getInt("SRCDSTPAIRID");
                        SrcDstPair srcDstPair = srcDstPairs.get(srcDstPairId);
                        if (srcDstPair != null) {
                            srcDstPairsToReturn.add(srcDstPair);
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
        return srcDstPairsToReturn;
    }

    public void savePairOrig(MacAddress srcMac, MacAddress dstMac, NodeConnector srcNodeConnector, NodeConnector dstNodeConnector, int routeIdSrcDst) {
        int srcMacId = srcMac.hashCode();
        int dstMacId = dstMac.hashCode();
        int srcNodeConnectorId = srcNodeConnector.hashCode();
        int dstNodeConnectorId = dstNodeConnector.hashCode();
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

    public int insertSrcDstPair(SrcDstPair srcDstPair) {
        int srcDstPairId = srcDstPair.hashCode();
        SrcDstPair oldValue = srcDstPairs.put(srcDstPairId, srcDstPair);
        if (!srcDstPair.equals(oldValue)) {
            Statement statement = null;
            try {
                try {
                    statement = conn.createStatement();
                    String insertFlowSql = "MERGE INTO SRCDSTPAIR("
                            + "ID, SRCMAC, DSTMAC, SRCNODECON, DSTNODECON) VALUES( "
                            + srcDstPairId + ", "
                            + srcDstPair.getSrcEth().hashCode() + ", "
                            + srcDstPair.getDstEth().hashCode() + ", "
                            + srcDstPair.getSrcNodeConnector().hashCode() + ", "
                            + srcDstPair.getDstNodeConnector().hashCode() + ")";
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
        return srcDstPairId;
    }

    public void insertEdgesFor(SrcDstPair srcDstPair, Path path) {
        int srcDstPairId = insertSrcDstPair(srcDstPair);
        try {
            Statement statement = conn.createStatement();
            try {
                for (Edge edge : path.getEdges()) {
                    int edgeId = edge.hashCode();
                    edges.put(edgeId, edge);
                    String insertFlowSql = "MERGE INTO EDGES("
                            + "EDGE, SRCDSTPAIRID) VALUES( "
                            + edgeId + ", "
                            + srcDstPairId + ")";
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

    public void insertFlows(HashMap<Node, List<FlowBuilder>> flowsWithGroups,
                            SrcDstPair srcDstPair) {
        int srcDstPairId = insertSrcDstPair(srcDstPair);
        try {
            Statement statement = conn.createStatement();
            try {
                for (Map.Entry<Node, List<FlowBuilder>> flowsOfNodes : flowsWithGroups.entrySet()) {
                    for (FlowBuilder fb : flowsOfNodes.getValue()) {
                        long flowId = fb.getCookie().longValue();
                        FlowNode flowNode = new FlowNode(flowId, flowsOfNodes.getKey());
                        flowNodes.put(flowId, flowNode);
                        flows.put(flowId, fb);
                        String insertFlowSql = "MERGE INTO FLOWS("
                                + "FLOWNODE, SRCDSTPAIRID) VALUES( "
                                + flowId + ", "
                                + srcDstPairId + ")";
                        statement.executeUpdate(insertFlowSql);
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
    }

    public void insertFlow(FlowBuilder fb, SrcDstPair srcDstPair) {
        FlowNode flowNode = new FlowNode(fb.getCookie().longValue(), srcDstPair.getSrcNodeConnector().getNode());
        int srcDstPairId = insertSrcDstPair(srcDstPair);
        try {
            Statement statement = conn.createStatement();
            try {
                long flowId = fb.getCookie().longValue();
                flowNodes.put(flowId, flowNode);
                flows.put(flowId, fb);
                String insertFlowSql = "MERGE INTO FLOWS("
                        + "FLOWNODE, SRCDSTPAIRID) VALUES( "
                        + flowId + ", "
                        + srcDstPairId + ")";
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

    public void saveEdgesFor(SrcDstPair srcDstPair,
                             Collection<List<Path>> cPaths) {
        int srcDstPairId = insertSrcDstPair(srcDstPair);
        try {
            Statement statement = conn.createStatement();
            try {
                for (List<Path> lPaths : cPaths) {
                    for (Path path : lPaths) {
                        for (Edge edge : path.getEdges()) {
                            int edgeId = edge.hashCode();
                            edges.put(edgeId, edge);
                            String insertFlowSql = "MERGE INTO EDGES("
                                    + "EDGE, SRCDSTPAIRID) VALUES( "
                                    + edgeId + ", "
                                    + srcDstPairId + ")";
                            statement.executeUpdate(insertFlowSql);
                        }
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
    }

    public SrcDstPair getSrcDstPair(long cookieId) {
        SrcDstPair toReturn = null;
        StringBuilder sb = new StringBuilder("SELECT DISTINCT SRCDSTPAIRID ");
        sb.append("FROM FLOWS ");
        sb.append("WHERE FLOWNODE = ").append(cookieId).append(";");
        try {
            Statement statement = conn.createStatement();
            String insertFlowSql = sb.toString();
            try {
                ResultSet resultSet = statement.executeQuery(insertFlowSql);
                if (resultSet.next()) {
                    try {
                        int srcDstPairId = resultSet.getInt("SRCDSTPAIRID");
                        toReturn = srcDstPairs.get(srcDstPairId);
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

    public HashSet<Long> getAllFlowIdsFromSrcDstPair(SrcDstPair sdp) {
        StringBuilder sb = new StringBuilder("SELECT DISTINCT FLOWNODE ");
        sb.append("FROM FLOWS ");
        sb.append("WHERE SRCDSTPAIRID = ").append(sdp.hashCode()).append(";");
        String searchFlow = sb.toString();
        HashSet<Long> flowIdsToReturn = new HashSet<>();

        try {
            Statement statement = conn.createStatement();
            try {
                ResultSet resultSet = statement.executeQuery(searchFlow);
                while (resultSet.next()) {
                    long flowId = resultSet.getLong("FLOWNODE");
                    flowIdsToReturn.add(flowId);
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
        return flowIdsToReturn;
    }

    public EthFlowsFromPathResult getEthFlowsFromPath(NodeConnector srcNodeConnector,
                                                      NodeConnector dstNodeConnector,
                                                      MacAddress srcMac,
                                                      MacAddress dstMac) {
        StringBuilder sb = new StringBuilder("SELECT FLOWS.FLOWNODE, SRCDSTPAIR.ID ");
        sb.append("FROM FLOWS,SRCDSTPAIR ");
        sb.append("WHERE SRCDSTPAIR.ID = FLOWS.SRCDSTPAIRID AND ");
        sb.append("SRCDSTPAIR.SRCMAC = ");
        sb.append(srcMac.hashCode());
        sb.append(" AND SRCDSTPAIR.DSTMAC = ");
        sb.append(dstMac.hashCode());
        sb.append(" AND SRCDSTPAIR.DSTNODECON = ");
        sb.append(dstNodeConnector.hashCode());
        sb.append(";");
        EthFlowsFromPathResult toReturn = new EthFlowsFromPathResult();
        try {
            Statement statement = conn.createStatement();
            String queryFromSql = sb.toString();
            try {
                ResultSet resultSet = statement.executeQuery(queryFromSql);
                while (resultSet.next()) {
                    try {
                        long flowId = resultSet.getLong("FLOWNODE");
                        int srcDstParId = resultSet.getInt("SRCDSTPAIR.ID");
                        FlowNode flowNode = flowNodes.get(flowId);
                        if (flowNode != null) {
                            if (flowNode.getNode().equals(srcNodeConnector.getNode())) {
                                FlowBuilder get = flows.get(flowNode.getFlowId());
                                if (get != null) {
                                    toReturn.getlFbs().add(get);
                                    SrcDstPair get1 = srcDstPairs.get(srcDstParId);
                                    if (get1 != null) {
                                        toReturn.setSrcNodeConnector(get1.getSrcNodeConnector());
                                    }
                                }
                            }
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

    private void createSchema() throws SQLException {
        Statement statement = null;
        String srcDstPairTable = "CREATE TABLE SRCDSTPAIR("
                + "ID INT NOT NULL, "
                + "SRCMAC INT NOT NULL, "
                + "DSTMAC INT NOT NULL, "
                + "SRCNODECON INT NOT NULL, "
                + "DSTNODECON INT NOT NULL, "
                + "PRIMARY KEY(ID))";
        String flowsTable = "CREATE TABLE FLOWS("
                + "FLOWNODE BIGINT NOT NULL, "
                + "SRCDSTPAIRID INT NOT NULL, "
                + "PRIMARY KEY (FLOWNODE,SRCDSTPAIRID), "
                + "FOREIGN KEY(SRCDSTPAIRID) REFERENCES SRCDSTPAIR(ID))";
        String edgesTable = "CREATE TABLE EDGES("
                + "EDGE INT NOT NULL, "
                + "SRCDSTPAIRID INT NOT NULL, "
                + "PRIMARY KEY (EDGE,SRCDSTPAIRID), "
                + "FOREIGN KEY(SRCDSTPAIRID) REFERENCES SRCDSTPAIR(ID))";
        String groupsTable = "CREATE TABLE GROUPS("
                + "NODE INT NOT NULL, "
                + "PORTIN INT NOT NULL, "
                + "PORTOUT INT NOT NULL, "
                + "GROUPID INT NOT NULL, "
                + "PRIMARY KEY (NODE,PORTIN,PORTOUT) "
                + ")";
        String cookiesTable = "CREATE TABLE COOKIES("
                + "COOKIEID BIGINT NOT NULL, "
                + "MATCHID INT NOT NULL, "
                + "PRIMARY KEY (MATCHID) "
                + ")";
        try {
            statement = conn.createStatement();
            statement.executeUpdate(srcDstPairTable);
            statement.executeUpdate(flowsTable);
            statement.executeUpdate(edgesTable);
            statement.executeUpdate(groupsTable);
            statement.executeUpdate(cookiesTable);
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    InstanceIdentifier<Flow> getInstanceIdentifier(Long next) {
        return iifs.get(next);
    }

    InstanceIdentifier<Group> getInstanceIdentifier(NodeId nid, GroupId gid) {
        long id = ((long) nid.hashCode() << 32) | (long) gid.hashCode();
        return iig.get(id);
    }

    InstanceIdentifier<Group> removeInstanceIdentifier(NodeId nid, GroupId gid) {
        long id = ((long) nid.hashCode() << 32) | (long) gid.hashCode();
        return iig.remove(id);
    }

    InstanceIdentifier<Flow> removeInstanceIdentifier(Long cookie) {
        return iifs.remove(cookie);
    }

    void saveInstanceIdentifier(Long cookie, InstanceIdentifier<Flow> instIdFlow) {
        iifs.put(cookie, instIdFlow);
    }

    void saveInstanceIdentifier(NodeId nid, GroupId gid, InstanceIdentifier<Group> instIdGroup) {
        long id = ((long) nid.hashCode() << 32) | (long) gid.hashCode();
        iig.put(id, instIdGroup);
    }

    void removeEdgesForSrcPairId(SrcDstPair sdp) {
        if (sdp == null) {
            return;
        }
        int srcDstPairId = sdp.hashCode();
        try {
            Statement statement = conn.createStatement();
            try {
                String insertFlowSql = "DELETE FROM EDGES WHERE "
                        + "SRCDSTPAIRID = " + srcDstPairId;
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

    void removeSrcPairId(SrcDstPair sdp) {
        if (sdp == null) {
            return;
        }
        removeEdgesForSrcPairId(sdp);
        int srcDstPairId = sdp.hashCode();
        StringBuilder sb = new StringBuilder("SELECT FLOWNODE ");
        sb.append("FROM FLOWS ");
        sb.append("WHERE SRCDSTPAIRID = ");
        sb.append(srcDstPairId);
        sb.append(";");
        try {
            Statement statement = conn.createStatement();
            String queryFromSql = sb.toString();
            try {
                ResultSet resultSet = statement.executeQuery(queryFromSql);
                while (resultSet.next()) {
                    try {
                        long flowId = resultSet.getLong("FLOWNODE");
                        flowNodes.remove(flowId);
                        flows.remove(flowId);
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

        try {
            Statement statement = conn.createStatement();
            try {
                String insertFlowSql = "DELETE FROM FLOWS WHERE "
                        + "SRCDSTPAIRID = " + srcDstPairId;
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
        srcDstPairs.remove(srcDstPairId);
    }

    public Path insertPath(SrcDstPair srcDstPair, Path primaryRoute) {
        return paths.put(srcDstPair, primaryRoute);
    }

    public Path getPath(SrcDstPair next) {
        return paths.get(next);
    }

    public Path removePath(SrcDstPair next) {
        return paths.remove(next);
    }

    public Collection<SrcDstPair> getAllSrcDstPair() {
        return srcDstPairs.values();
    }

    public Long getCookieFromMatch(Match match) {
        int hashCodeMatch = getHashCodeMatch(match);
        Long toReturn = null;
        StringBuilder sb = new StringBuilder("SELECT DISTINCT COOKIEID ");
        sb.append("FROM COOKIES ");
        sb.append("WHERE MATCHID = ").append(hashCodeMatch).append(";");
        try {
            Statement statement = conn.createStatement();
            String insertFlowSql = sb.toString();
            try {
                ResultSet resultSet = statement.executeQuery(insertFlowSql);
                if (resultSet.next()) {
                    try {
                        toReturn = resultSet.getLong("COOKIEID");
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

    public void saveCookie(Match match, long cookieId) {
//        Long cookieFromMatch = getCookieFromMatch(match);
//        HashSet<Long> hs = new HashSet<Long>();
//        hs.add(cookieFromMatch);
//        removeCookies(hs);
        int hashCodeMatch = getHashCodeMatch(match);
        Statement statement = null;
        try {
            try {
                statement = conn.createStatement();
                String insertFlowSql = "MERGE INTO COOKIES("
                        + "COOKIEID, MATCHID) KEY(MATCHID) VALUES( "
                        + cookieId + ", "
                        + hashCodeMatch + ")";
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

    public void removeCookies(HashSet<Long> cookies) {
        StringBuilder sb = new StringBuilder("DELETE FROM COOKIES ");
        sb.append("WHERE COOKIEID=");
        for (Long l : cookies) {
            if (l != null) {
                sb.append(l);
                sb.append(" OR COOKIEID=");
            }
        }
        sb.delete(sb.length() - " OR COOKIEID=".length(), sb.length());
        sb.append(";");
        try {
            Statement statement = conn.createStatement();
            try {
                String insertFlowSql = sb.toString();
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

    private int getHashCodeMatch(Match match) {
        MacAddress srcMac = null;
        if (match.getEthernetMatch().getEthernetSource() != null) {
            srcMac = match.getEthernetMatch().getEthernetSource().getAddress();
        }
        MacAddress dstMac = match.getEthernetMatch().getEthernetDestination().getAddress();
        NodeConnectorId inPort = match.getInPort();
        final int prime = 31;
        int result = 1;
        result = prime * result + ((inPort == null) ? 0 : inPort.hashCode());
        result = prime * result + ((srcMac == null) ? 0 : srcMac.hashCode());
        result = prime * result + ((dstMac == null) ? 0 : dstMac.hashCode());
        return result;
    }
}
