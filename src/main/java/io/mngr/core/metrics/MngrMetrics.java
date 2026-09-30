package io.mngr.core.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;

/** Micrometer 封装（§10 命名）。 */
@RequiredArgsConstructor
public class MngrMetrics {

    private final MeterRegistry registry;

    public void publishSuccess(String protocol) {
        registry.counter("mngr_publish_total", "protocol", protocol, "result", "success").increment();
    }

    public void publishFail(String protocol) {
        registry.counter("mngr_publish_total", "protocol", protocol, "result", "fail").increment();
    }

    public void consume(String protocol, String serverId) {
        registry.counter("mngr_consume_total", "protocol", protocol, "server", serverId).increment();
    }

    public void templateMatched(String type) {
        registry.counter("mngr_template_match_total", "type", type).increment();
    }

    public void unclassified() {
        registry.counter("mngr_msg_unclassified_total").increment();
    }

    public void decodeFail() {
        registry.counter("mngr_msg_decode_fail_total").increment();
    }

    public void dlq(String origin) {
        registry.counter("mngr_dlq_total", "origin", origin).increment();
    }

    public void stateChange(String protocol) {
        registry.counter("mngr_conn_state_total", "protocol", protocol).increment();
    }

    public void handlerFail(String serverId) {
        registry.counter("mngr_handler_fail_total", "server", serverId).increment();
    }
}
