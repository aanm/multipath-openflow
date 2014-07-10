package org.opendaylight.multiroute.experimenter.test;

import java.nio.ByteBuffer;
import javax.xml.bind.DatatypeConverter;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;

/**
 *
 * @author aanm
 */
public class HexStringTester {

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    public HexStringTester() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void convertMacAddressToByte() {
        MacAddress mac = new MacAddress("12:34:56:78:90:AB");
        MacAddress mac2 = new MacAddress("CD:EF:01:23:45:56");
        byte[] parseHexBinaryMac = DatatypeConverter.parseHexBinary(mac.getValue().replaceAll(":", ""));
        byte[] parseHexBinaryMac2 = DatatypeConverter.parseHexBinary(mac2.getValue().replaceAll(":", ""));
        byte[] putLong = ByteBuffer.allocate(8).putLong(0x00001234567890ABL).array();
        byte[] putLong2 = ByteBuffer.allocate(8).putLong(0x0000CDEF01234556L).array();

        assertArrayEquals(ByteBuffer.allocate(6).put(putLong, 2, 6).array(), parseHexBinaryMac);
        assertArrayEquals(ByteBuffer.allocate(6).put(putLong2, 2, 6).array(), parseHexBinaryMac2);
    }
}
