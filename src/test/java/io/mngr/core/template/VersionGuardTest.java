package io.mngr.core.template;

import io.mngr.core.model.FieldDef;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VersionGuardTest {

    private static FieldDef f(String name, String type, boolean req) {
        return FieldDef.builder().name(name).type(type).required(req).build();
    }

    @Test
    void addingOptionalFieldIsNonBreaking() {
        VersionGuard.Diff d = VersionGuard.diff(
                List.of(f("a", "string", true)),
                List.of(f("a", "string", true), f("b", "number", false)));
        assertFalse(d.breaking());
        assertTrue(d.changes().contains("新增字段 b"));
    }

    @Test
    void removingFieldIsBreaking() {
        VersionGuard.Diff d = VersionGuard.diff(
                List.of(f("a", "string", false)),
                List.of());
        assertTrue(d.breaking());
        assertTrue(d.changes().contains("删除字段 a"));
    }

    @Test
    void typeChangeIsBreaking() {
        VersionGuard.Diff d = VersionGuard.diff(
                List.of(f("a", "string", false)),
                List.of(f("a", "integer", false)));
        assertTrue(d.breaking());
    }

    @Test
    void requiredTighteningIsBreaking() {
        VersionGuard.Diff d = VersionGuard.diff(
                List.of(f("a", "string", false)),
                List.of(f("a", "string", true)));
        assertTrue(d.breaking());
        assertTrue(d.changes().stream().anyMatch(c -> c.contains("收紧为必填")));
    }

    @Test
    void nestedChildRemovalReportedWithPath() {
        FieldDef objOld = FieldDef.builder().name("o").type("object")
                .children(List.of(f("x", "string", true))).build();
        FieldDef objNew = FieldDef.builder().name("o").type("object")
                .children(List.of()).build();
        VersionGuard.Diff d = VersionGuard.diff(List.of(objOld), List.of(objNew));
        assertTrue(d.breaking());
        assertTrue(d.changes().stream().anyMatch(c -> c.contains("删除字段 o.x")));
    }
}
