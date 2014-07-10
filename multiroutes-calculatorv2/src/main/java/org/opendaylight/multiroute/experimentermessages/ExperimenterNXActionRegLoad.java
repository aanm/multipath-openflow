package org.opendaylight.multiroute.experimentermessages;

import java.nio.ByteBuffer;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.experimenter.action._case.ExperimenterActionBuilder;

public class ExperimenterNXActionRegLoad {

    /* Action structure for NXAST_REG_LOAD.
     *
     * Copies value[0:n_bits] to dst[ofs:ofs+n_bits], where a[b:c] denotes the bits
     * within 'a' numbered 'b' through 'c' (not including bit 'c').  Bit numbering
     * starts at 0 for the least-significant bit, 1 for the next most significant
     * bit, and so on.
     *
     * 'dst' is an nxm_header with nxm_hasmask=0.  See the documentation for
     * NXAST_REG_MOVE, above, for the permitted fields and for the side effects of
     * loading them.
     *
     * The 'ofs' and 'n_bits' fields are combined into a single 'ofs_nbits' field
     * to avoid enlarging the structure by another 8 bytes.  To allow 'n_bits' to
     * take a value between 1 and 64 (inclusive) while taking up only 6 bits, it is
     * also stored as one less than its true value:
     *
     *  15                           6 5                0
     * +------------------------------+------------------+
     * |              ofs             |    n_bits - 1    |
     * +------------------------------+------------------+
     *
     * The switch will reject actions for which ofs+n_bits is greater than the
     * width of 'dst', or in which any bits in 'value' with value 2**n_bits or
     * greater are set to 1, with error type OFPET_BAD_ACTION, code
     * OFPBAC_BAD_ARGUMENT.
     */
//    struct nx_action_reg_load {
//    ovs_be16 type;                  /* OFPAT_VENDOR. */
//    ovs_be16 len;                   /* Length is 24. */
//    ovs_be32 vendor;                /* NX_VENDOR_ID. */
//    ovs_be16 subtype;               /* NXAST_REG_LOAD. */
//    ovs_be16 ofs_nbits;             /* (ofs << 6) | (n_bits - 1). */
//    ovs_be32 dst;                   /* Destination register. */
//    ovs_be64 value;                 /* Immediate value. */
//};
//OFP_ASSERT(sizeof(struct nx_action_reg_load) == 24);
    private static final long NX_VENDOR_ID = 0x00002320;
    private static final int type = 0x0007;
    private final int w;
    private long value;
    private final int registerNumber;
    private int ofs_nbits;

    public ExperimenterNXActionRegLoad(int registerNumber, boolean w) throws IllegalArgumentException {
        if (registerNumber < 0 || registerNumber > 8) {
            throw new IllegalArgumentException("Register number is not valid. Only between 0 and 7");
        }
        this.registerNumber = registerNumber;
        if (w) {
            this.w = 1;
        } else {
            this.w = 0;
        }
    }

    public void loadValueToRegister(long value, int firstBit, int lastBit) throws IllegalArgumentException {
        if ((firstBit < 0 || firstBit > 31) || (lastBit < 0 || lastBit > 31)
                || lastBit <= firstBit) {
            throw new IllegalArgumentException("Bit number's not valid");
        }
        this.value = value;
        this.ofs_nbits = firstBit << 6 | ((lastBit - firstBit));
    }

    public ExperimenterActionBuilder build() {
        byte[] arrayType = ByteBuffer.allocate(4).putInt(type).array();
        byte[] arrayOfs_nbits = ByteBuffer.allocate(4).putInt(ofs_nbits).array();
        int reg = ((0x0001 << 16) | (registerNumber << 9) | (w << 8) | 4);
        byte[] data = ByteBuffer.allocate(16).put(arrayType, 2, 2).put(arrayOfs_nbits, 2, 2).
                putInt(reg).putLong(value).array();
        ExperimenterActionBuilder eab = new ExperimenterActionBuilder();
        eab.setExperimenter(NX_VENDOR_ID);
        eab.setData(data);
        return eab;
    }
}
