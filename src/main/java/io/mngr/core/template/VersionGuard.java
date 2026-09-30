package io.mngr.core.template;

import io.mngr.core.model.FieldDef;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** 版本兼容门禁（FR-6.4）：字段删除 / 类型变更 / 必填收紧 = 破坏性。 */
public final class VersionGuard {

    private VersionGuard() {}

    public record Diff(boolean breaking, List<String> changes) {}

    public static Diff diff(List<FieldDef> oldFields, List<FieldDef> newFields) {
        List<String> changes = new ArrayList<>();
        boolean[] breaking = {false};
        compare(oldFields, newFields, "", changes, breaking);
        return new Diff(breaking[0], changes);
    }

    private static void compare(List<FieldDef> oldF, List<FieldDef> newF, String prefix,
                                List<String> changes, boolean[] breaking) {
        List<FieldDef> o = oldF == null ? List.of() : oldF;
        List<FieldDef> n = newF == null ? List.of() : newF;
        for (FieldDef of : o) {
            String path = path(prefix, of.getName());
            FieldDef nf = find(n, of.getName());
            if (nf == null) {
                changes.add("删除字段 " + path);
                if (of.isRequired()) breaking[0] = true;
                else breaking[0] = true; // 字段删除即破坏性（需求定义）
                continue;
            }
            if (!Objects.equals(of.getType(), nf.getType())) {
                changes.add(path + " 类型 " + of.getType() + " → " + nf.getType());
                breaking[0] = true;
            }
            if (!of.isRequired() && nf.isRequired()) {
                changes.add(path + " 收紧为必填");
                breaking[0] = true;
            }
            if (nf.isContainer() || of.isContainer()) {
                compare(of.getChildren(), nf.getChildren(), path, changes, breaking);
            }
        }
        for (FieldDef nf : n) {
            if (find(o, nf.getName()) == null) {
                changes.add("新增字段 " + path(prefix, nf.getName()));
            }
        }
    }

    static String path(String prefix, String name) {
        return prefix.isEmpty() ? name : prefix + "." + name;
    }

    static FieldDef find(List<FieldDef> defs, String name) {
        return defs.stream().filter(d -> name.equals(d.getName())).findFirst().orElse(null);
    }
}
