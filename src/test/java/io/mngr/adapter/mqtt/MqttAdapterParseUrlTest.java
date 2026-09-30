package io.mngr.adapter.mqtt;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MqttAdapterParseUrlTest {

    @Test
    void tcpSchemeStripped() {
        MqttAdapter.HostPort hp = MqttAdapter.parseUrl("tcp://localhost:1883");
        assertEquals("localhost", hp.host());
        assertEquals(1883, hp.port());
    }

    @Test
    void plainHostPort() {
        MqttAdapter.HostPort hp = MqttAdapter.parseUrl("10.0.0.1:1884");
        assertEquals("10.0.0.1", hp.host());
        assertEquals(1884, hp.port());
    }

    @Test
    void hostOnlyDefaults() {
        assertEquals(1883, MqttAdapter.parseUrl("broker.local").port());
        assertEquals("broker.local", MqttAdapter.parseUrl("broker.local").host());
        assertEquals(8883, MqttAdapter.parseUrl("mqtts://broker.local").port());
        assertEquals(8883, MqttAdapter.parseUrl("ssl://broker.local").port());
        assertEquals(8883, MqttAdapter.parseUrl("tls://broker.local").port());
    }

    @Test
    void pathAndWsSchemes() {
        MqttAdapter.HostPort hp = MqttAdapter.parseUrl("mqtt://host:1883/mqtt");
        assertEquals("host", hp.host());
        assertEquals(1883, hp.port());
        MqttAdapter.HostPort ws = MqttAdapter.parseUrl("ws://host:8080");
        assertEquals("host", ws.host());
        assertEquals(8080, ws.port());
        assertEquals(8883, MqttAdapter.parseUrl("wss://host").port());
    }
}
