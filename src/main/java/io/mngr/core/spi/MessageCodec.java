package io.mngr.core.spi;

import com.fasterxml.jackson.databind.JsonNode;
import io.mngr.core.model.FieldDef;

import java.util.List;

/** 消息编解码（FR-5）。本期仅 json。 */
public interface MessageCodec {
    String name();

    /** 解析失败抛 MngrException(5001)。 */
    JsonNode decode(byte[] payload);

    byte[] encode(JsonNode node);

    /** 模板驱动转换：类型矫正、默认值填充、必填校验。返回违规列表（空=通过）。 */
    List<String> validateAgainstTemplate(JsonNode node, List<FieldDef> fields);
}
