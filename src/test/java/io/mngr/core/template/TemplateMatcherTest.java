package io.mngr.core.template;

import com.fasterxml.jackson.databind.JsonNode;
import io.mngr.core.enums.MatchType;
import io.mngr.core.enums.TemplateStatus;
import io.mngr.core.model.FieldDef;
import io.mngr.core.model.TemplateDef;
import io.mngr.store.StoreJson;
import io.mngr.support.InMemoryConfigRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TemplateMatcherTest {

    private InMemoryConfigRepository repo;
    private TemplateMatcher matcher;

    private static JsonNode n(String json) throws Exception {
        return StoreJson.mapper().readTree(json);
    }

    private void tpl(String id, TemplateStatus status, FieldDef... fields) {
        repo.putTemplate(TemplateDef.builder().id(id).version(1).name(id)
                .status(status).fields(List.of(fields)).build());
    }

    private static FieldDef f(String name, String type) {
        return FieldDef.builder().name(name).type(type).build();
    }

    @BeforeEach
    void setUp() {
        repo = new InMemoryConfigRepository();
        matcher = new TemplateMatcher(repo, TemplateMatcher.DEFAULT_THRESHOLD);
    }

    @Test
    void explicitHeaderWinsEvenIfTemplateUnbound() throws Exception {
        tpl("t1", TemplateStatus.ENABLED, f("a", "string"));
        JsonNode msg = n("{\"a\":\"x\"}");
        TemplateMatcher.MatchResult r = matcher.match("t1@1", List.of(), msg);
        assertEquals(MatchType.EXPLICIT, r.getType());
        assertEquals("t1", r.getTemplateId());
        assertEquals(1.0, r.getScore());
    }

    @Test
    void singleBoundIdHitsDirectly() throws Exception {
        tpl("t1", TemplateStatus.ENABLED, f("a", "string"));
        TemplateMatcher.MatchResult r = matcher.match(null, List.of("t1"), n("{\"zzz\":1}"));
        assertEquals(MatchType.BOUND, r.getType());
    }

    @Test
    void similarityScansAllEnabledWhenNoBound() throws Exception {
        tpl("t1", TemplateStatus.ENABLED, f("a", "string"), f("b", "number"), f("c", "boolean"));
        tpl("t2", TemplateStatus.DISABLED, f("x", "string"));
        TemplateMatcher.MatchResult r = matcher.match(null, null, n("{\"a\":\"1\",\"b\":2.5,\"c\":true}"));
        assertEquals(MatchType.SIMILARITY, r.getType());
        assertEquals("t1", r.getTemplateId());
        assertEquals(1.0, r.getScore(), 1e-9);
    }

    @Test
    void similarityBelowThresholdGivesNone() throws Exception {
        tpl("t1", TemplateStatus.ENABLED, f("a", "string"), f("b", "number"), f("c", "boolean"), f("d", "string"));
        TemplateMatcher.MatchResult r = matcher.match(null, null, n("{\"a\":\"1\",\"q\":2}"));
        assertEquals(MatchType.NONE, r.getType());
    }

    @Test
    void emptyBoundListMeansNoScan() throws Exception {
        tpl("t1", TemplateStatus.ENABLED, f("a", "string"));
        TemplateMatcher.MatchResult r = matcher.match(null, List.of(), n("{\"a\":\"x\"}"));
        assertEquals(MatchType.NONE, r.getType());
    }

    @Test
    void numericCrossTypeScoresZeroPointEight() {
        double s = TemplateMatcher.scoreOne(List.of("v:integer"), List.of("v:number"));
        assertEquals(0.8, s, 1e-9);
        assertEquals(1.0, TemplateMatcher.scoreOne(List.of("v:integer"), List.of("v:any")), 1e-9);
        assertEquals(0.0, TemplateMatcher.scoreOne(List.of("v:integer"), List.of("v:string")), 1e-9);
    }

    @Test
    void invalidateClearsFingerprintCache() throws Exception {
        tpl("t1", TemplateStatus.ENABLED, f("a", "string"));
        JsonNode msg = n("{\"a\":\"x\"}");
        assertEquals(MatchType.SIMILARITY, matcher.match(null, null, msg).getType());
        repo.updateTemplateStatus("t1", 1, TemplateStatus.DISABLED);
        matcher.invalidate("t1");
        assertEquals(MatchType.NONE, matcher.match(null, null, msg).getType());
    }
}
