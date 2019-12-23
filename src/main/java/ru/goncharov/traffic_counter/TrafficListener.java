package ru.goncharov.traffic_counter;

import org.pcap4j.core.*;
import org.pcap4j.packet.Packet;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static ru.goncharov.traffic_counter.Constant.*;

public class TrafficListener implements Runnable, Closeable {

    // IP-address for traffic counting
    private String trafficNetAddress;
    // Net interfaces
    private List<PcapNetworkInterface> interfaceList;
    // Pcap handles of net interfaces
    private List<PcapHandle> handleList = new ArrayList<>();

    // Bytes caught
    private final AtomicInteger bytes = new AtomicInteger(0);
    // Was limits reached
    private AtomicBoolean minLimitReached = new AtomicBoolean(false);
    private AtomicBoolean maxLimitReached = new AtomicBoolean(false);

    // Is open listener
    private boolean isOpen = true;
    private Producer producer;
    private Limit limit;
    // Timer for refresh reached limits
    private Timer refreshLimitsReachedTimer = new Timer(true);

    public TrafficListener(Producer producer, Limit limit) throws PcapNativeException {
        this.producer = producer;
        this.limit = limit;

        // Find all net devices
        interfaceList = Pcaps.findAllDevs();
        // If devices was not found
        if (interfaceList == null || interfaceList.isEmpty()) {
            System.err.println("Not found network interfaces");
            System.exit(1);
        }
    }

    public TrafficListener(Producer producer, Limit limit, String trafficHost) throws PcapNativeException {
        this(producer, limit);
        this.trafficNetAddress = trafficHost;
    }

    @Override
    public void run() {
        // If net interfaces was not found
        if (interfaceList.isEmpty()) {
            Main.printErrorAndExit("Net interfaces not found");
        }
        // Start handle for all net interfaces
        for (PcapNetworkInterface device : interfaceList) {
            startHandleThread(device);
        }
        // If handles was not started
        if (handleList.isEmpty()) {
            Main.printErrorAndExit("Handles not exist");
        }
        startRefreshTimer();
    }

    // Start timer for refresh reached limits
    private void startRefreshTimer() {
        int period = 5*60*1000; // 5 minutes
        TimerTask refreshLimitsReachedTask = new TimerTask() {
            @Override
            public void run() {
                refreshReachedLimits();
            }
        };
        refreshLimitsReachedTimer.scheduleAtFixedRate(refreshLimitsReachedTask, 0, period);
    }

    // Refresh reached limits
    private void refreshReachedLimits() {
        bytes.set(0);
        minLimitReached.set(false);
        maxLimitReached.set(false);
    }

    // Start handle thread for count traffic from net interface
    private void startHandleThread(PcapNetworkInterface device) {
        try {
            // Package length in bytes
            int snapshotLength = 65536;
            // Read timeout in milliseconds
            int readTimeout = 50;
            final PcapHandle handle =
                    device.openLive(snapshotLength, PcapNetworkInterface.PromiscuousMode.PROMISCUOUS, readTimeout);

            // If net address for count was received, set filter
            if (trafficNetAddress != null) {
                String filter = "net " + trafficNetAddress;
                try {
                    handle.setFilter(filter, BpfProgram.BpfCompileMode.OPTIMIZE);
                } catch (NotOpenException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            // Create and start thread
            Runnable listenerLoop = () -> {
                handleLoop(handle);
            };
            Thread loopThread = new Thread(listenerLoop);
            loopThread.setDaemon(true);
            loopThread.start();

            handleList.add(handle);
        } catch (PcapNativeException ex) {
            ex.printStackTrace();
        }
    }

    // Handle received packet
    private void handlePacket(Packet packet) {
        // Sum packet length
        int tmpBytes = bytes.addAndGet(packet.length());
        // If min limit is reached
        if (!minLimitReached.get()
                && tmpBytes >= limit.getMinLim()) {
            minLimitReached.set(true);
            // Send message to topic
            producer.sendAlertTopic(MIN_LIMIT_MESSAGE);
        }
        // If max limit is reached
        if (minLimitReached.get()
                && !maxLimitReached.get()
                && tmpBytes>= limit.getMaxLim()) {
            maxLimitReached.set(true);
            // Send message to topic
            producer.sendAlertTopic(MAX_LIMIT_MESSAGE);
        }
    }

    // Activate loop of handle for catching packets
    private void handleLoop(PcapHandle handle) {
        try {
            int maxPackets = -1;
            handle.loop(maxPackets, this::handlePacket);

        } catch (InterruptedException | NotOpenException | PcapNativeException e) {
            if (isOpen) {
                e.printStackTrace();
            }
        } finally {
            handle.close();
        }
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
        refreshLimitsReachedTimer.cancel();
    }

// ----Getters/Setters-------------------------

    public AtomicInteger getBytes() {
        return bytes;
    }
}
