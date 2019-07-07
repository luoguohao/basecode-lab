package com.luogh.learning.lab.common;


import static org.objectweb.asm.Type.getConstructorDescriptor;
import static org.objectweb.asm.Type.getMethodDescriptor;

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class LambdaChecker {

  public static void main(String[] args) {
    MapFunction<Object, Boolean> func = Objects::nonNull;
    java.util.function.Function<Object, Boolean> func1 = Objects::isNull;

    for (Object obj : Lists.newArrayList(func1, func)) {
      System.out
          .println("======" + obj.getClass() + "->" + (obj instanceof Serializable) + "=========");
      for (Method method : getAllDeclaredMethods(obj.getClass())) {
        System.out.println(method.getModifiers() + " " + method.getReturnType().getSimpleName()
            + " " + method.getName() + "(" + Arrays.stream(method.getParameterTypes()).map(
            Class::getSimpleName).collect(
            Collectors.joining()) + ")");
      }
    }

    LambdaExecutable executable = checkAndExtractLambda(func);

    System.out.println("lambda getName:" + executable.getName());
    System.out.println("lambda getReturnType Name:" + executable.getReturnType().getTypeName());
    System.out.println("lambda getReturnType Name:" + Arrays
        .toString(executable.getParameterTypes()));
  }


  public static LambdaExecutable checkAndExtractLambda(Function function) {
    try {
      // get serialized lambda
      SerializedLambda serializedLambda = null;
      for (Class<?> clazz = function.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
        try {
          Method replaceMethod = clazz.getDeclaredMethod("writeReplace");
          replaceMethod.setAccessible(true);
          Object serialVersion = replaceMethod.invoke(function);

          // check if class is a lambda function
          if (serialVersion != null && serialVersion.getClass() == SerializedLambda.class) {
            serializedLambda = (SerializedLambda) serialVersion;
            break;
          }
        } catch (NoSuchMethodException e) {
          // thrown if the method is not there. fall through the loop
        }
      }

      // not a lambda method -> return null
      if (serializedLambda == null) {
        return null;
      }

      // find lambda method
      String className = serializedLambda.getImplClass(); // 对应到lambda中具体的实现类
      String methodName = serializedLambda.getImplMethodName(); // 对应到lambda中实现类的方法
      String methodSig = serializedLambda.getImplMethodSignature(); // 对应到lambda中实现类的方法签名

      Class<?> implClass = Class.forName(className.replace('/', '.'), true,
          Thread.currentThread().getContextClassLoader());

      // find constructor
      if (methodName.equals("<init>")) {
        Constructor<?>[] constructors = implClass.getDeclaredConstructors();
        for (Constructor<?> constructor : constructors) {
          if (getConstructorDescriptor(constructor).equals(methodSig)) {
            return new LambdaExecutable(constructor);
          }
        }
      }
      // find method
      else {
        List<Method> methods = getAllDeclaredMethods(implClass);
        for (Method method : methods) {
          if (method.getName().equals(methodName) && getMethodDescriptor(method)
              .equals(methodSig)) {
            return new LambdaExecutable(method);
          }
        }
      }
      throw new RuntimeException("No lambda method found.");
    } catch (Exception e) {
      throw new RuntimeException("Could not extract lambda method out of function: " +
          e.getClass().getSimpleName() + " - " + e.getMessage(), e);
    }
  }

  /**
   * Returns all declared methods of a class including methods of superclasses.
   */
  public static List<Method> getAllDeclaredMethods(Class<?> clazz) {
    List<Method> result = new ArrayList<>();
    while (clazz != null) {
      Method[] methods = clazz.getDeclaredMethods();
      Collections.addAll(result, methods);
      clazz = clazz.getSuperclass();
    }
    return result;
  }
}
