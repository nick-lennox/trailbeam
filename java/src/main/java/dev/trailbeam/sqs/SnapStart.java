package dev.trailbeam.sqs;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;

/**
 * Lambda SnapStart: one snapshot of an initialized function becomes many execution environments. Each must have
 * its own call-order scope (contract section 2a) and {@code service.instance.id} (section 5), so after a restore
 * the jar starts both again (QV-312 report, section 3, point 6), and before the snapshot it flushes what it holds.
 *
 * <p>The hook is the {@code org.crac} API that SnapStart runs, which AWS tells SnapStart users to add
 * ({@code io.github.crac:org-crac}). The jar doesn't depend on it: if {@code org.crac.Core} is on the classpath,
 * a {@code org.crac.Resource} is registered through reflection; if it isn't, nothing happens, and a function can
 * call {@link Trailbeam#afterRestore()} from its own hook.
 */
final class SnapStart {
  static final long FLUSH_TIMEOUT_MS = 2000;

  /** {@code org.crac} keeps only weak references to registered resources, so the jar holds this one. */
  private static volatile Object resource;

  private SnapStart() {}

  static synchronized void register() {
    if (resource != null) {
      return;
    }
    try {
      ClassLoader cl = SnapStart.class.getClassLoader();
      Class<?> core = Class.forName("org.crac.Core", true, cl);
      Class<?> resourceType = Class.forName("org.crac.Resource", true, cl);
      Object context = core.getMethod("getGlobalContext").invoke(null);
      Object r = proxy(resourceType);
      context.getClass().getMethod("register", resourceType).invoke(context, r);
      resource = r;
    } catch (ClassNotFoundException | LinkageError e) {
      resource = Boolean.FALSE; // no org.crac: nothing to register, and nothing to try again
    } catch (Exception e) {
      resource = Boolean.FALSE;
      Trailbeam.LOG.log(System.Logger.Level.DEBUG, "trailbeam: could not register the SnapStart hook", e);
    }
  }

  /** An {@code org.crac.Resource} whose two methods call {@link #beforeCheckpoint} and {@link #afterRestore}. */
  static Object proxy(Class<?> resourceType) {
    InvocationHandler h = (self, method, args) -> invoke(self, method, args);
    return Proxy.newProxyInstance(resourceType.getClassLoader(), new Class<?>[] {resourceType}, h);
  }

  private static Object invoke(Object self, Method method, Object[] args) {
    switch (method.getName()) {
      case "beforeCheckpoint":
        beforeCheckpoint();
        return null;
      case "afterRestore":
        afterRestore();
        return null;
      case "hashCode":
        return System.identityHashCode(self);
      case "equals":
        return self == args[0];
      case "toString":
        return "trailbeam SnapStart hook";
      default:
        return null;
    }
  }

  static void beforeCheckpoint() {
    try {
      Trailbeam.flush(FLUSH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (RuntimeException e) {
      Trailbeam.LOG.log(System.Logger.Level.DEBUG, "trailbeam: flush before the snapshot failed", e);
    }
  }

  static void afterRestore() {
    try {
      Trailbeam.afterRestore();
    } catch (RuntimeException e) {
      Trailbeam.LOG.log(System.Logger.Level.DEBUG, "trailbeam: restore hook failed", e);
    }
  }
}
