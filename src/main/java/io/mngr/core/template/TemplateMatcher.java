package io.mngr.core.template;

import com.fasterxml.jackson.databind.JsonNode;
import io.mngr.codec.json.JsonCodec;
import io.mngr.core.enums.MatchType;
import io.mngr.core.enums.TemplateStatus;
import io.mngr.core.model.TemplateDef;
import io.mngr.core.spi.ConfigRepository;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.*;

/** 匹配短路链（§7.3）：EXPLICIT → BOUND → SIMILARITY（打分+LRU）→ NONE。 */
public class TemplateMatcher {

    public static final double DEFAULT_THRESHOLD = 0.8;

    @Data
    @AllArgsConstructor
    public static class MatchResult {
        private String templateId;
        private Integer version;
        private MatchType type;
        private Double score;
        public static MatchResult none() { return new MatchResult(null, null, MatchType.NONE, null); }
    }

    private final ConfigRepository repo;
    private final double threshold;
    private final Map<String, MatchResult> lru;

    public TemplateMatcher(ConfigRepository repo, double threshold) {
        this.repo = repo;
        this.threshold = threshold > 0 ? threshold : DEFAULT_THRESHOLD;
        this.lru = Collections.synchronizedMap(new LinkedHashMap<>(1024, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, MatchResult> e) { return size() > 1024; }
        });
    }

    /**
     * @param explicitHeader headers["x-template"]，形如 id 或 id@version
     * @param boundIds       订阅/Topic 绑定的模板候选集（可为空）
     * @param node           已解析消息
     */
    public MatchResult match(String explicitHeader, List<String> boundIds, JsonNode node) {
        // 1. EXPLICIT
        if (explicitHeader != null && !explicitHeader.isBlank()) {
            String id = explicitHeader;
            int ver = 0;
            int at = explicitHeader.indexOf('@');
            if (at > 0) {
                id = explicitHeader.substring(0, at);
                try { ver = Integer.parseInt(explicitHeader.substring(at + 1)); } catch (NumberFormatException ignored) { }
            }
            Optional<TemplateDef> t = ver > 0 ? repo.findTemplate(id, ver) : repo.findTemplate(id);
            if (t.isPresent()) {
                return new MatchResult(t.get().getId(), t.get().getVersion(), MatchType.EXPLICIT, 1.0);
            }
        }

        // 2. BOUND：单绑定直接命中
        if (boundIds != null && boundIds.size() == 1) {
            Optional<TemplateDef> t = repo.findTemplate(boundIds.get(0));
            if (t.isPresent()) {
                return new MatchResult(t.get().getId(), t.get().getVersion(), MatchType.BOUND, 1.0);
            }
        }

        // 3. SIMILARITY：指纹缓存
        List<String> msgPaths = JsonCodec.flattenPaths(node);
        // boundIds==null → 全量启用模板；空列表 → 不做相似度扫描
        if (boundIds != null && boundIds.isEmpty()) return MatchResult.none();
        List<String> scope = boundIds == null ? enabledTemplateIds() : boundIds;
        String scopeKey = String.join(",", new TreeSet<>(scope));
        String fp = StructureFingerprint.ofMessage(msgPaths) + "#" + scopeKey;
        MatchResult cached = lru.get(fp);
        if (cached != null) return cached;

        MatchResult best = score(scope, msgPaths, boundIds != null && !boundIds.isEmpty());
        lru.put(fp, best);
        return best;
    }

    private List<String> enabledTemplateIds() {
        List<String> ids = new ArrayList<>();
        for (TemplateDef t : repo.listTemplates(null, 0, 10000)) {
            if (t.getStatus() == TemplateStatus.ENABLED) ids.add(t.getId());
        }
        return ids;
    }

    private MatchResult score(List<String> candidateIds, List<String> msgPaths, boolean isBound) {
        MatchResult best = MatchResult.none();
        double bestScore = 0;
        for (String id : candidateIds) {
            Optional<TemplateDef> opt = repo.findTemplate(id);
            if (opt.isEmpty() || opt.get().getStatus() != TemplateStatus.ENABLED) continue;
            TemplateDef t = opt.get();
            double s = scoreOne(msgPaths, JsonCodec.templatePaths(t.getFields()));
            if (s >= threshold && (s > bestScore
                    || (s == bestScore && best.getTemplateId() != null
                        && t.getVersion() > repo.findTemplate(best.getTemplateId(), best.getVersion())
                              .map(TemplateDef::getVersion).orElse(0)))) {
                bestScore = s;
                best = new MatchResult(t.getId(), t.getVersion(),
                        isBound ? MatchType.BOUND : MatchType.SIMILARITY, s);
            }
        }
        return best;
    }

    /** score = Σ hit(path)*w_type / max(|msg|,|tpl|)；w: 同型 1.0，number↔integer 0.8，object↔array 0.5。 */
    static double scoreOne(List<String> msgPaths, List<String> tplPaths) {
        if (msgPaths.isEmpty() || tplPaths.isEmpty()) return 0;
        Map<String, String> msgByPath = new HashMap<>();
        for (String p : msgPaths) {
            int c = p.lastIndexOf(':');
            msgByPath.put(p.substring(0, c), p.substring(c + 1));
        }
        double sum = 0;
        for (String tp : tplPaths) {
            int c = tp.lastIndexOf(':');
            String path = tp.substring(0, c), tt = tp.substring(c + 1);
            String mt = msgByPath.get(path);
            if (mt == null) continue;
            double w;
            if (mt.equals(tt) || "any".equals(tt) || "any".equals(mt)) w = 1.0;
            else if (isNum(mt) && isNum(tt)) w = 0.8;
            else if (isContainer(mt) && isContainer(tt)) w = 0.5;
            else continue;
            sum += w;
        }
        return sum / Math.max(msgPaths.size(), tplPaths.size());
    }

    static boolean isNum(String t) { return "number".equals(t) || "integer".equals(t); }
    static boolean isContainer(String t) { return "object".equals(t) || "array".equals(t); }

    /** 模板变更后失效全部指纹缓存（缓存 key 含模板集，按 id 精确失效需反查集合，成本高，整体清空）。 */
    public void invalidate(String templateId) {
        lru.clear();
    }
}
