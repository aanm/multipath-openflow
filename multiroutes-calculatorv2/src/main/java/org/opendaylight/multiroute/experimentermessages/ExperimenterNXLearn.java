package org.opendaylight.multiroute.experimentermessages;

import java.nio.ByteBuffer;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.experimenter.action._case.ExperimenterActionBuilder;

public class ExperimenterNXLearn {


    /* Action structure for NXAST_LEARN.
     *
     * This action adds or modifies a flow in an OpenFlow table, similar to
     * OFPT_FLOW_MOD with OFPFC_MODIFY_STRICT as 'command'.  The new flow has the
     * specified idle timeout, hard timeout, priority, cookie, and flags.  The new
     * flow's match criteria and actions are built by applying each of the series
     * of flow_mod_spec elements included as part of the action.
     *
     * A flow_mod_spec starts with a 16-bit header.  A header that is all-bits-0 is
     * a no-op used for padding the action as a whole to a multiple of 8 bytes in
     * length.  Otherwise, the flow_mod_spec can be thought of as copying 'n_bits'
     * bits from a source to a destination.  In this case, the header contains
     * multiple fields:
     *
     *  15  14  13 12  11 10                              0
     * +------+---+------+---------------------------------+
     * |   0  |src|  dst |             n_bits              |
     * +------+---+------+---------------------------------+
     *
     * The meaning and format of a flow_mod_spec depends on 'src' and 'dst'.  The
     * following table summarizes the meaning of each possible combination.
     * Details follow the table:
     *
     *   src dst  meaning
     *   --- ---  ----------------------------------------------------------
     *    0   0   Add match criteria based on value in a field.
     *    1   0   Add match criteria based on an immediate value.
     *    0   1   Add NXAST_REG_LOAD action to copy field into a different field.
     *    1   1   Add NXAST_REG_LOAD action to load immediate value into a field.
     *    0   2   Add OFPAT_OUTPUT action to output to port from specified field.
     *   All other combinations are undefined and not allowed.
     *
     * The flow_mod_spec header is followed by a source specification and a
     * destination specification.  The format and meaning of the source
     * specification depends on 'src':
     *
     *   - If 'src' is 0, the source bits are taken from a field in the flow to
     *     which this action is attached.  (This should be a wildcarded field.  If
     *     its value is fully specified then the source bits being copied have
     *     constant values.)
     *
     *     The source specification is an ovs_be32 'field' and an ovs_be16 'ofs'.
     *     'field' is an nxm_header with nxm_hasmask=0, and 'ofs' the starting bit
     *     offset within that field.  The source bits are field[ofs:ofs+n_bits-1].
     *     'field' and 'ofs' are subject to the same restrictions as the source
     *     field in NXAST_REG_MOVE.
     *
     *   - If 'src' is 1, the source bits are a constant value.  The source
     *     specification is (n_bits+15)/16*2 bytes long.  Taking those bytes as a
     *     number in network order, the source bits are the 'n_bits'
     *     least-significant bits.  The switch will report an error if other bits
     *     in the constant are nonzero.
     *
     * The flow_mod_spec destination specification, for 'dst' of 0 or 1, is an
     * ovs_be32 'field' and an ovs_be16 'ofs'.  'field' is an nxm_header with
     * nxm_hasmask=0 and 'ofs' is a starting bit offset within that field.  The
     * meaning of the flow_mod_spec depends on 'dst':
     *
     *   - If 'dst' is 0, the flow_mod_spec specifies match criteria for the new
     *     flow.  The new flow matches only if bits field[ofs:ofs+n_bits-1] in a
     *     packet equal the source bits.  'field' may be any nxm_header with
     *     nxm_hasmask=0 that is allowed in NXT_FLOW_MOD.
     *
     *     Order is significant.  Earlier flow_mod_specs must satisfy any
     *     prerequisites for matching fields specified later, by copying constant
     *     values into prerequisite fields.
     *
     *     The switch will reject flow_mod_specs that do not satisfy NXM masking
     *     restrictions.
     *
     *   - If 'dst' is 1, the flow_mod_spec specifies an NXAST_REG_LOAD action for
     *     the new flow.  The new flow copies the source bits into
     *     field[ofs:ofs+n_bits-1].  Actions are executed in the same order as the
     *     flow_mod_specs.
     *
     *     A single NXAST_REG_LOAD action writes no more than 64 bits, so n_bits
     *     greater than 64 yields multiple NXAST_REG_LOAD actions.
     *
     * The flow_mod_spec destination spec for 'dst' of 2 (when 'src' is 0) is
     * empty.  It has the following meaning:
     *
     *   - The flow_mod_spec specifies an OFPAT_OUTPUT action for the new flow.
     *     The new flow outputs to the OpenFlow port specified by the source field.
     *     Of the special output ports with value OFPP_MAX or larger, OFPP_IN_PORT,
     *     OFPP_FLOOD, OFPP_LOCAL, and OFPP_ALL are supported.  Other special ports
     *     may not be used.
     *
     * Resource Management
     * -------------------
     *
     * A switch has a finite amount of flow table space available for learning.
     * When this space is exhausted, no new learning table entries will be learned
     * until some existing flow table entries expire.  The controller should be
     * prepared to handle this by flooding (which can be implemented as a
     * low-priority flow).
     *
     * If a learned flow matches a single TCP stream with a relatively long
     * timeout, one may make the best of resource constraints by setting
     * 'fin_idle_timeout' or 'fin_hard_timeout' (both measured in seconds), or
     * both, to shorter timeouts.  When either of these is specified as a nonzero
     * value, OVS adds a NXAST_FIN_TIMEOUT action, with the specified timeouts, to
     * the learned flow.
     *
     * Examples
     * --------
     *
     * The following examples give a prose description of the flow_mod_specs along
     * with informal notation for how those would be represented and a hex dump of
     * the bytes that would be required.
     *
     * These examples could work with various nx_action_learn parameters.  Typical
     * values would be idle_timeout=OFP_FLOW_PERMANENT, hard_timeout=60,
     * priority=OFP_DEFAULT_PRIORITY, flags=0, table_id=10.
     *
     * 1. Learn input port based on the source MAC, with lookup into
     *    NXM_NX_REG1[16:31] by resubmit to in_port=99:
     *
     *    Match on in_port=99:
     *       ovs_be16(src=1, dst=0, n_bits=16),               20 10
     *       ovs_be16(99),                                    00 63
     *       ovs_be32(NXM_OF_IN_PORT), ovs_be16(0)            00 00 00 02 00 00
     *
     *    Match Ethernet destination on Ethernet source from packet:
     *       ovs_be16(src=0, dst=0, n_bits=48),               00 30
     *       ovs_be32(NXM_OF_ETH_SRC), ovs_be16(0)            00 00 04 06 00 00
     *       ovs_be32(NXM_OF_ETH_DST), ovs_be16(0)            00 00 02 06 00 00
     *
     *    Set NXM_NX_REG1[16:31] to the packet's input port:
     *       ovs_be16(src=0, dst=1, n_bits=16),               08 10
     *       ovs_be32(NXM_OF_IN_PORT), ovs_be16(0)            00 00 00 02 00 00
     *       ovs_be32(NXM_NX_REG1), ovs_be16(16)              00 01 02 04 00 10
     *
     *    Given a packet that arrived on port A with Ethernet source address B,
     *    this would set up the flow "in_port=99, dl_dst=B,
     *    actions=load:A->NXM_NX_REG1[16..31]".
     *
     *    In syntax accepted by ovs-ofctl, this action is: learn(in_port=99,
     *    NXM_OF_ETH_DST[]=NXM_OF_ETH_SRC[],
     *    load:NXM_OF_IN_PORT[]->NXM_NX_REG1[16..31])
     *
     * 2. Output to input port based on the source MAC and VLAN VID, with lookup
     *    into NXM_NX_REG1[16:31]:
     *
     *    Match on same VLAN ID as packet:
     *       ovs_be16(src=0, dst=0, n_bits=12),               00 0c
     *       ovs_be32(NXM_OF_VLAN_TCI), ovs_be16(0)           00 00 08 02 00 00
     *       ovs_be32(NXM_OF_VLAN_TCI), ovs_be16(0)           00 00 08 02 00 00
     *
     *    Match Ethernet destination on Ethernet source from packet:
     *       ovs_be16(src=0, dst=0, n_bits=48),               00 30
     *       ovs_be32(NXM_OF_ETH_SRC), ovs_be16(0)            00 00 04 06 00 00
     *       ovs_be32(NXM_OF_ETH_DST), ovs_be16(0)            00 00 02 06 00 00
     *
     *    Output to the packet's input port:
     *       ovs_be16(src=0, dst=2, n_bits=16),               10 10
     *       ovs_be32(NXM_OF_IN_PORT), ovs_be16(0)            00 00 00 02 00 00
     *
     *    Given a packet that arrived on port A with Ethernet source address B in
     *    VLAN C, this would set up the flow "dl_dst=B, vlan_vid=C,
     *    actions=output:A".
     *
     *    In syntax accepted by ovs-ofctl, this action is:
     *    learn(NXM_OF_VLAN_TCI[0..11], NXM_OF_ETH_DST[]=NXM_OF_ETH_SRC[],
     *    output:NXM_OF_IN_PORT[])
     *
     * 3. Here's a recipe for a very simple-minded MAC learning switch.  It uses a
     *    10-second MAC expiration time to make it easier to see what's going on
     *
     *      ovs-vsctl del-controller br0
     *      ovs-ofctl del-flows br0
     *      ovs-ofctl add-flow br0 "table=0 actions=learn(table=1, \
     hard_timeout=10, NXM_OF_VLAN_TCI[0..11],             \
     NXM_OF_ETH_DST[]=NXM_OF_ETH_SRC[],                   \
     output:NXM_OF_IN_PORT[]), resubmit(,1)"
     *      ovs-ofctl add-flow br0 "table=1 priority=0 actions=flood"
     *
     *    You can then dump the MAC learning table with:
     *
     *      ovs-ofctl dump-flows br0 table=1
     *
     * Usage Advice
     * ------------
     *
     * For best performance, segregate learned flows into a table that is not used
     * for any other flows except possibly for a lowest-priority "catch-all" flow
     * (a flow with no match criteria).  If different learning actions specify
     * different match criteria, use different tables for the learned flows.
     *
     * The meaning of 'hard_timeout' and 'idle_timeout' can be counterintuitive.
     * These timeouts apply to the flow that is added, which means that a flow with
     * an idle timeout will expire when no traffic has been sent *to* the learned
     * address.  This is not usually the intent in MAC learning; instead, we want
     * the MAC learn entry to expire when no traffic has been sent *from* the
     * learned address.  Use a hard timeout for that.
     */
    private static final long NX_VENDOR_ID = 0x00002320;
    private static final int pad = 0;
    private static final int type = 0x0010;

    private int idle_timeout = 0;
    private int hard_timeout = 0;
    private int priority = 0x8000;
    private int fin_idle_timeout = 0;
    private int fin_hard_timeout = 0;

    private long cookie = 0;
    private int flags = 0;
    private int table_id = 1;
    private ByteBuffer actionsByteBuffer = null;

    public ExperimenterNXLearn() {
        actionsByteBuffer = ByteBuffer.allocate(0);
    }

    public void setFin_idle_timeout(int fin_idle_timeout) {
        this.fin_idle_timeout = fin_idle_timeout;
    }

    public void setFin_hard_timeout(int fin_hard_timeout) {
        this.fin_hard_timeout = fin_hard_timeout;
    }

    public void setIdle_timeout(int idle_timeout) {
        this.idle_timeout = idle_timeout;
    }

    public void setHard_timeout(int hard_timeout) {
        this.hard_timeout = hard_timeout;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public void setCookie(long cookie) {
        this.cookie = cookie;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public void setTable_id(int table_id) {
        this.table_id = table_id;
    }

    public void setOutputFromRegister(int registerNumber, int firstBit, int lastBit) throws IllegalArgumentException {
        // 10 1d 00 01 08 04 00 03 output = NXM_NX_REG4[3..31]
        // 10 17 00 01 08 04 00 03 output = NXM_NX_REG4[3..25]
        // 10 19 00 01 08 04 00 01 output = NXM_NX_REG4[1..25]
        // 10 19 00 01 02 04 00 01 output = NXM_NX_REG1[1..25]
        if ((firstBit < 0 || firstBit > 31) || (lastBit < 0 || lastBit > 31)
                || lastBit <= firstBit) {
            throw new IllegalArgumentException("Bit number's not valid");
        }
        int offset = lastBit - firstBit + 1;
        int begin = 0x10 << 24 | (0x000000FF & offset) << 16 | 0x01;
        int end = (registerNumber * 2) << 24 | 0x04 << 16 | firstBit;
        ByteBuffer actionsByteBufferTemp = ByteBuffer.allocate(actionsByteBuffer.capacity() + 8)
                .put(actionsByteBuffer.array())
                .putInt(begin)
                .putInt(end);
        actionsByteBuffer = actionsByteBufferTemp;
    }

    public void setPortIn(int portNumber) {
        // 20 10 12 34 00 00 00 02 00 00 PORTIN = 0x1234
        int codeA = 0x2010;
        int codeB = 0x0002;
        ByteBuffer bbCodeA = ByteBuffer.allocate(4).putInt(codeA);
        ByteBuffer bbPortNumber = ByteBuffer.allocate(4).putInt(portNumber);
        ByteBuffer bbZeros = ByteBuffer.allocate(4).putInt(0);
        ByteBuffer actionsByteBufferTemp = ByteBuffer.allocate(actionsByteBuffer.capacity() + 10)
                .put(actionsByteBuffer.array())
                .put(bbCodeA.array(), 2, 2)
                .put(bbPortNumber.array(), 2, 2)
                .putInt(codeB)
                .put(bbZeros.array(), 2, 2);
        actionsByteBuffer = actionsByteBufferTemp;

    }

    public void setEthSrc(byte[] src) {
        // 20 30 66 56 78 90 ab cd 00 00 04 06 00 00 ETH_SRC = 66:56:78:90:AB:CD
        int codeA = 0x2030;
        int codeB = 0x0406;
        ByteBuffer bbCodeA = ByteBuffer.allocate(4).putInt(codeA);
        ByteBuffer bbZeros = ByteBuffer.allocate(4).putInt(0);
        ByteBuffer actionsByteBufferTemp = ByteBuffer.allocate(actionsByteBuffer.capacity() + 14)
                .put(actionsByteBuffer.array())
                .put(bbCodeA.array(), 2, 2)
                .put(src)
                .putInt(codeB)
                .put(bbZeros.array(), 2, 2);
        actionsByteBuffer = actionsByteBufferTemp;
    }

    public void setEthDst(byte[] dst) {
        // 20 30 00 99 00 34 56 78 00 00 02 06 00 00 ETH_DST = 00:99:00:34:56:78
        int codeA = 0x2030;
        int codeB = 0x0206;
        ByteBuffer bbCodeA = ByteBuffer.allocate(4).putInt(codeA);
        ByteBuffer bbZeros = ByteBuffer.allocate(4).putInt(0);
        ByteBuffer actionsByteBufferTemp = ByteBuffer.allocate(actionsByteBuffer.capacity() + 14)
                .put(actionsByteBuffer.array())
                .put(bbCodeA.array(), 2, 2)
                .put(dst)
                .putInt(codeB)
                .put(bbZeros.array(), 2, 2);
        actionsByteBuffer = actionsByteBufferTemp;
    }

    public ExperimenterActionBuilder build() {
        byte[] arrayType = ByteBuffer.allocate(4).putInt(type).array();
        byte[] arrayIdle_timeout = ByteBuffer.allocate(4).putInt(idle_timeout).array();
        byte[] arrayHard_timeout = ByteBuffer.allocate(4).putInt(hard_timeout).array();
        byte[] arrayPriority = ByteBuffer.allocate(4).putInt(priority).array();
        byte[] arrayFlags = ByteBuffer.allocate(4).putInt(flags).array();
        byte[] arrayTable_id = ByteBuffer.allocate(4).putInt(table_id).array();
        byte[] arrayPad = ByteBuffer.allocate(4).putInt(pad).array();
        byte[] arrayFin_idle_timeout = ByteBuffer.allocate(4).putInt(fin_idle_timeout).array();
        byte[] arrayFin_hard_timeout = ByteBuffer.allocate(4).putInt(fin_hard_timeout).array();
        int actionsLength = actionsByteBuffer.capacity();
        int paddingZeros = 8 - (actionsLength % 8);
        byte[] bbPaddingZeros = ByteBuffer.allocate(8).putLong(0).array();
        byte[] data = ByteBuffer.allocate(24 + actionsLength + paddingZeros)
                .put(arrayType, 2, 2)
                .put(arrayIdle_timeout, 2, 2)
                .put(arrayHard_timeout, 2, 2)
                .put(arrayPriority, 2, 2)
                .putLong(cookie)
                .put(arrayFlags, 2, 2)
                .put(arrayTable_id, 3, 1)
                .put(arrayPad, 3, 1)
                .put(arrayFin_idle_timeout, 2, 2)
                .put(arrayFin_hard_timeout, 2, 2)
                .put(actionsByteBuffer.array())
                .put(bbPaddingZeros, 0, paddingZeros)
                .array();
        ExperimenterActionBuilder eab = new ExperimenterActionBuilder();
        eab.setExperimenter(NX_VENDOR_ID);
        eab.setData(data);
        return eab;
    }
}
