package com.luogh.learning.lab.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import lombok.Data;

/**
 * Similar to a Java 8 Executable but with a return type.
 */
@Data
public class LambdaExecutable {

  private Type[] parameterTypes;
  private Type returnType;
  private String name;
  private Object executable;

  public LambdaExecutable(Constructor<?> constructor) {
    this.parameterTypes = constructor.getGenericParameterTypes();
    this.returnType = constructor.getDeclaringClass();
    this.name = constructor.getName();
    this.executable = constructor;
  }

  public LambdaExecutable(Method method) {
    this.parameterTypes = method.getGenericParameterTypes();
    this.returnType = method.getGenericReturnType();
    this.name = method.getName();
    this.executable = method;
  }

  public Type[] getParameterTypes() {
    return parameterTypes;
  }

  public Type getReturnType() {
    return returnType;
  }

  public String getName() {
    return name;
  }

  public boolean executablesEquals(Method m) {
    return executable.equals(m);
  }

  public boolean executablesEquals(Constructor<?> c) {
    return executable.equals(c);
  }
}