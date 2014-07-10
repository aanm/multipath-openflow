package org.opendaylight.multiroute.experimenter.test;

import java.nio.ByteBuffer;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opendaylight.multiroute.experimentermessages.ExperimenterNXActionRegLoad;
import org.opendaylight.multiroute.experimentermessages.ExperimenterNXLearn;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.experimenter.action._case.ExperimenterActionBuilder;

/**
 *
 * @author aanm
 */
public class TestExperimenterAction {

    public TestExperimenterAction() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void createExperimenterNXLearn1() {
        ExperimenterNXLearn enxl;
        enxl = new ExperimenterNXLearn();
//        enxl.setCookie(0);
//        enxl.setFin_hard_timeout(0);
//        enxl.setFin_idle_timeout(0);
//        enxl.setFlags(0);
//        enxl.setHard_timeout(0);
//        enxl.setHeader(ExperimenterNXLearn.HeaderCombination.REG_LOAD_TO_COPY);
//        enxl.setIdle_timeout(0);
        enxl.setTable_id(0);
        enxl.setPortIn(2);
        byte[] ethDst = ByteBuffer.allocate(8).putLong(0x0000999999999999L).array();
        enxl.setEthDst(ByteBuffer.allocate(6).put(ethDst, 2, 6).array());
        byte[] ethSrc = ByteBuffer.allocate(8).putLong(0x000066567890ABCDL).array();
        enxl.setEthSrc(ByteBuffer.allocate(6).put(ethSrc, 2, 6).array());
        enxl.setOutputFromRegister(1, 1, 25);
//        enxl.setPriority(0);
        ExperimenterActionBuilder build = enxl.build();
        long experimenterL = build.getExperimenter();
        int experimenter = (int) experimenterL;
        byte[] actual = ByteBuffer.allocate(4 + build.getData().length).putInt(experimenter).put(build.getData()).array();

        int a = 0x00002320;
        long b = 0x0010000000008000L;
        long c = 0x0000000000000000L;
        long d = 0x0000000000000000L;
        long e = 0x2010000200000002L;
        long f = 0x0000203099999999L;
        long g = 0x9999000002060000L;
        long h = 0x203066567890abcdL;
        long i = 0x0000040600001019L;
        long j = 0x0001020400010000L;
        byte[] arrayResult = ByteBuffer.allocate(4 + 9 * 8)
                .putInt(a)
                .putLong(b)
                .putLong(c)
                .putLong(d)
                .putLong(e)
                .putLong(f)
                .putLong(g)
                .putLong(h)
                .putLong(i)
                .putLong(j)
                .array();
        assertArrayEquals(arrayResult, actual);

    }
    @Test
    public void createExperimenterNXLearn2() {
        ExperimenterNXLearn enxl;
        enxl = new ExperimenterNXLearn();
//        enxl.setCookie(0);
//        enxl.setFin_hard_timeout(0);
//        enxl.setFin_idle_timeout(0);
//        enxl.setFlags(0);
//        enxl.setHard_timeout(0);
//        enxl.setHeader(ExperimenterNXLearn.HeaderCombination.REG_LOAD_TO_COPY);
        enxl.setIdle_timeout(10);
//        enxl.setTable_id(0);
        enxl.setPriority(3388);
        enxl.setOutputFromRegister(3, 5, 20);
        byte[] ethDst = ByteBuffer.allocate(8).putLong(0x0000999999999999L).array();
        enxl.setEthDst(ByteBuffer.allocate(6).put(ethDst, 2, 6).array());
        enxl.setPortIn(99);
        byte[] ethSrc = ByteBuffer.allocate(8).putLong(0x000066567890ABCDL).array();
        enxl.setEthSrc(ByteBuffer.allocate(6).put(ethSrc, 2, 6).array());
        ExperimenterActionBuilder build = enxl.build();
        long experimenterL = build.getExperimenter();
        int experimenter = (int) experimenterL;
        byte[] actual = ByteBuffer.allocate(4 + build.getData().length).putInt(experimenter).put(build.getData()).array();

        int a = 0x00002320;
        long b = 0x0010000a00000d3cL;
        long c = 0x0000000000000000L;
        long d = 0x0000010000000000L;
        long e = 0x1010000106040005L;
        long f = 0x2030999999999999L;
        long g = 0x0000020600002010L;
        long h = 0x0063000000020000L;
        long i = 0x203066567890abcdL;
        long j = 0x0000040600000000L;
        byte[] arrayResult = ByteBuffer.allocate(4 + 9 * 8)
                .putInt(a)
                .putLong(b)
                .putLong(c)
                .putLong(d)
                .putLong(e)
                .putLong(f)
                .putLong(g)
                .putLong(h)
                .putLong(i)
                .putLong(j)
                .array();
        assertArrayEquals(arrayResult, actual);

    }

    @Test
    public void createExperimenterNXActionRegLoad() {
        ExperimenterNXActionRegLoad enxarl;
        enxarl = new ExperimenterNXActionRegLoad(1, false);
        enxarl.loadValueToRegister(0xffff, 1, 31);
        ExperimenterActionBuilder build = enxarl.build();
        long experimenterL = build.getExperimenter();
        int experimenter = (int) experimenterL;
        byte[] actual = ByteBuffer.allocate(20).putInt(experimenter).put(build.getData()).array();

        int a = 0x00002320;
        long b = 0x0007005e00010204L;
        long c = 0x000000000000ffffL;
        byte[] arrayResult = ByteBuffer.allocate(20).putInt(a).putLong(b).putLong(c).array();
        assertArrayEquals(arrayResult, actual);

        enxarl = new ExperimenterNXActionRegLoad(3, false);
        enxarl.loadValueToRegister(0x2422, 16, 31);
        build = enxarl.build();
        experimenterL = build.getExperimenter();
        experimenter = (int) experimenterL;
        actual = ByteBuffer.allocate(20).putInt(experimenter).put(build.getData()).array();
        b = 0x0007040f00010604L;
        c = 0x0000000000002422L;
        arrayResult = ByteBuffer.allocate(20).putInt(a).putLong(b).putLong(c).array();
        assertArrayEquals(arrayResult, actual);

        enxarl = new ExperimenterNXActionRegLoad(5, false);
        enxarl.loadValueToRegister(0x2422, 11, 24);
        build = enxarl.build();
        experimenterL = build.getExperimenter();
        experimenter = (int) experimenterL;
        actual = ByteBuffer.allocate(20).putInt(experimenter).put(build.getData()).array();
        b = 0x000702cd00010a04L;
        c = 0x0000000000002422L;
        arrayResult = ByteBuffer.allocate(20).putInt(a).putLong(b).putLong(c).array();
        assertArrayEquals(arrayResult, actual);
    }
}
