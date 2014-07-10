package org.opendaylight.multiroute.calculator;

import java.util.logging.Level;
import org.opendaylight.controller.hosttracker.IfIptoHost;
import org.opendaylight.controller.sal.binding.api.AbstractBindingAwareConsumer;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.ConsumerContext;
import org.opendaylight.controller.sal.binding.api.NotificationService;
import org.opendaylight.controller.sal.binding.api.data.DataBrokerService;
import org.opendaylight.controller.sal.routing.IDijkstraInterface;
import org.opendaylight.controller.sal.utils.ServiceHelper;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The activator for OSGi
 *
 * @author aanm@ua.pt
 * @version 0.1, 04/06/14
 */
public class Activator extends AbstractBindingAwareConsumer {

    /**
     * The logger instance to debug or sent some info about activator.
     */
    protected static final Logger logger = LoggerFactory
            .getLogger(Activator.class);

    /**
     * The MultiRouteImpl instance for the OSGi instance.
     */
    private MultiRouteImpl m = null;

    /**
     * Once the session of OSGi is initialized it must call this method in order
     * to setup instance's MultiRouteImpl
     *
     * @param session ConsumerContext session
     */
    @Override
    public void onSessionInitialized(ConsumerContext session) {
        logger.debug("onSessionInitialized start");
        m = new MultiRouteImpl();
        IfIptoHost i;
        do {
            i = (IfIptoHost) ServiceHelper.getGlobalInstance(IfIptoHost.class, this);
            try {
                Thread.sleep(2500);
            } catch (InterruptedException ex) {
            }
        } while (i == null);
        m.setHostFinder(i);
        IDijkstraInterface iDijkstra;
        do {
            iDijkstra = (IDijkstraInterface) ServiceHelper.getGlobalInstance(IDijkstraInterface.class, this);
            try {
                Thread.sleep(2500);
            } catch (InterruptedException ex) {
            }
        } while (iDijkstra == null);
        m.setDijkstraService(iDijkstra);
        m.setPacketProcessingService(session.getRpcService(PacketProcessingService.class));
        m.setNotificationService(session.getSALService(NotificationService.class));
        m.setDataBrokerService(session.getSALService(DataBrokerService.class));
        m.start();
    }

}
