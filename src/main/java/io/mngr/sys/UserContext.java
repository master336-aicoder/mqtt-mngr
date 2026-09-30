package io.mngr.sys;

/** 当前登录用户线程上下文。 */
public final class UserContext {
    private static final ThreadLocal<UserInfo> HOLDER = new ThreadLocal<>();

    public record UserInfo(Long userId, String username, String nickname) {}

    public static void set(UserInfo info) { HOLDER.set(info); }
    public static UserInfo get() { return HOLDER.get(); }
    public static void clear() { HOLDER.remove(); }

    public static String username() {
        UserInfo u = HOLDER.get();
        return u == null ? null : u.username();
    }
}
