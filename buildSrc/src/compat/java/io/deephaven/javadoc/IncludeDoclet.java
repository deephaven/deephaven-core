package io.deephaven.javadoc;// From http://www.sixlegs.com/blog/java/exclude-javadoc-tag.html

import com.sun.javadoc.*;
import com.sun.tools.doclets.standard.Standard;
import com.sun.tools.javadoc.Main;

import java.io.FileNotFoundException;
import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Allows for the custom javadoc tags
 * Include
 * IncludeAll
 * Exclude
 * With none of these tags, the javadoc will not be included in the produced document.
 *
 * IncludeAll- add at the beginning of a Class to include all methods, fields, etc. by default. Note inner classes must
 * have their own Include tag to be included.
 * Include- add to a class, method, or field to include inside the produced javadoc.
 * Exclude- add to a class, method, or field to exclude inside the produced javadoc. Only necessary after an IncludeAll tag
 */
public class IncludeDoclet extends com.sun.tools.doclets.standard.Standard {
    private static String INCLUDEALL = "IncludeAll";
    private static String INCLUDE = "Include";
    private static String EXCLUDE = "Exclude";
    public static void main(String[] args) throws FileNotFoundException {
        debug("main()");
        String name = IncludeDoclet.class.getName();
        Main.execute(name, args);
    }

    public static boolean start(RootDoc root)  {
        debug("start()");
        return Standard.start((RootDoc) process(root, RootDoc.class));
    }

    private static Object process(Object obj, Class<?> expect) {
        debug("process(%s, %s)", obj, expect);
        if (obj == null)
            return null;
        Class<?> cls = obj.getClass();
        if (cls.getName().startsWith("com.sun.")) {
            return Proxy.newProxyInstance(cls.getClassLoader(),
                    cls.getInterfaces(), new IncludeHandler(obj));
        } else if (obj instanceof Object[]) {
            Class<?> componentType = expect.getComponentType();
            Object[] array = (Object[]) obj;
            List<Object> list = new ArrayList<Object>(array.length);
            for (int i = 0; i < array.length; i++) {
                Object entry = array[i];
                if ((entry instanceof Doc) && includeAll((Doc) entry)) {
                    if(entry instanceof ClassDoc) {
                        list.add(processNeedExclude(entry, componentType));
                        continue;
                    }
                } else if ((entry instanceof Doc) && !include((Doc) entry)) {
                    continue;
                }
                list.add(process(entry, componentType));
            }
            return list.toArray((Object[]) Array.newInstance(componentType,
                    list.size()));
        } else {
            return obj;
        }
    }

    private static Object processNeedExclude(Object obj, Class<?> expect) {
        debug("process(%s, %s)", obj, expect);
        if (obj == null)
            return null;
        Class<?> cls = obj.getClass();
        if (cls.getName().startsWith("com.sun.")) {
            return Proxy.newProxyInstance(cls.getClassLoader(),
                    cls.getInterfaces(), new ExcludeHandler(obj));
        } else if (obj instanceof Object[]) {
            Class<?> componentType = expect.getComponentType();
            Object[] array = (Object[]) obj;
            List<Object> list = new ArrayList<Object>(array.length);
            for (int i = 0; i < array.length; i++) {
                Object entry = array[i];
                if ((entry instanceof Doc) && exclude((Doc) entry)) {
                    continue;
                }
                list.add(processNeedExclude(entry, componentType));
            }
            return list.toArray((Object[]) Array.newInstance(componentType,
                    list.size()));
        } else {
            return obj;
        }
    }

    private static boolean includeAll(Doc doc) {
        if (doc instanceof ProgramElementDoc) {
            if (((ProgramElementDoc) doc).containingPackage().tags(INCLUDEALL).length > 0) {
                debug("includeall(%s) returns true", doc.toString());
                return true;
            }
        }
        boolean result = doc.tags(INCLUDEALL).length > 0;
        debug("includeall(%s) returns true", doc.toString());
        return result;
    }

    private static boolean include(Doc doc) {
        if (doc instanceof ProgramElementDoc) {
            if (((ProgramElementDoc) doc).containingPackage().tags(INCLUDE).length > 0) {
                debug("include(%s) returns true", doc.toString());
                return true;
            }
        }
        boolean result = doc.tags(INCLUDE).length > 0;
        debug("include(%s) returns true", doc.toString());
        return result;
    }

    private static boolean exclude(Doc doc) {
        if (doc instanceof ProgramElementDoc) {
            if (((ProgramElementDoc) doc).containingPackage().tags(EXCLUDE).length > 0) {
                debug("exclude(%s) returns true", doc.toString());
                return true;
            }
        }
        //changed to include= maybe change back
        boolean result = doc.tags(EXCLUDE).length > 0;
        debug("exclude(%s) returns true", doc.toString());
        return result;
    }

    private static class IncludeHandler implements InvocationHandler {
        private Object target;

        public IncludeHandler(Object target) {
            this.target = target;
        }

        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {
            debug("invoke(%s, %s, %s)", "proxy", "method", args);

            if (args != null) {
                String methodName = method.getName();
                if (methodName.equals("compareTo")
                        || methodName.equals("equals")
                        || methodName.equals("overrides")
                        || methodName.equals("subclassOf")) {
                    args[0] = unwrap(args[0]);
                }
            }
            try {
                return process(method.invoke(target, args),
                        method.getReturnType());
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }

        private Object unwrap(Object proxy) {
            debug("unwrap(%s)", proxy);
            if (proxy instanceof Proxy) {
                if(Proxy.getInvocationHandler(proxy) instanceof IncludeHandler) {
                    return ((IncludeHandler) Proxy.getInvocationHandler(proxy)).target;
                } else {
                    return ((ExcludeHandler) Proxy.getInvocationHandler(proxy)).target;
                }
            }
            return proxy;
        }
    }

    private static class ExcludeHandler implements InvocationHandler {
        private Object target;

        public ExcludeHandler(Object target) {
            this.target = target;
        }

        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {
            debug("invokeExcludeHandler(%s, %s, %s)", "proxy", "method", args);

            if (args != null) {
                String methodName = method.getName();
                if (methodName.equals("compareTo")
                        || methodName.equals("equals")
                        || methodName.equals("overrides")
                        || methodName.equals("subclassOf")) {
                    args[0] = unwrap(args[0]);
                }
            }
            try {
                return processNeedExclude(method.invoke(target, args),
                        method.getReturnType());
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }

        private Object unwrap(Object proxy) {
            debug("unwrap(%s)", proxy);
            if (proxy instanceof Proxy) {
                if(Proxy.getInvocationHandler(proxy) instanceof IncludeHandler) {
                    return ((IncludeHandler) Proxy.getInvocationHandler(proxy)).target;
                } else {
                    return ((ExcludeHandler) Proxy.getInvocationHandler(proxy)).target;
                }
            }
            return proxy;
        }
    }

    private static void debug(String fmt, Object... o) {
        //  System.out.printf("IncludeDoclet: " + fmt + "\n", o);
    }
}
