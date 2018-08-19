package com.luogh.j2se.test;

import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.GETSTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.RETURN;
import static org.objectweb.asm.Opcodes.V1_8;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * 理解 invokedynamic
 *
 * invokedynamic和ASM字节码的应用
 *
 * inDy（invokedynamic）是 java 7 引入的一条新的虚拟机指令，这是自 1.0 以来第一次引入新的虚拟机指令。
 * 到了 java 8 这条指令才第一次在 java 应用，用在 lambda 表达式中。 indy 与其他 invoke 指令不同的是
 * 它允许由应用级的代码来决定方法解析。所谓应用级的代码其实是一个方法，在这里这个方法被称为
 * 引导方法（Bootstrap Method），简称 BSM。BSM 返回一个 CallSite（调用点） 对象，这个对象就和 inDy 链接在一起了。
 * 以后再执行这条 inDy 指令都不会创建新的 CallSite 对象。CallSite 就是一个 MethodHandle（方法句柄）的 holder。
 * 方法句柄指向一个调用点真正执行的方法。
 *
 * https://www.jianshu.com/p/d74e92f93752
 */
public class InvokeDynamicInstructionTest {

  /**
   * 这里只需要关心的是“mv.visitInvokeDynamicInsn(“toUpperCase”, “()Ljava/lang/String;”, BSM, “Hello”)；”这行代码。
   * 这行代码是用来在字节代码中生成invokedynamic指令的。在调用的时候传入了方法的名称、方法句柄的类型、对应的启动方法
   * 和额外的参数“Hello”。在invokedynamic指令被执行的时候，会先调用对应的启动方法，即代码清单中的bootstrap方法。
   * bootstrap方法的返回值是一个ConstantCallSite的对象。接着从该ConstantCallSite对象中通过getTarget方法获取
   * 目标方法句柄，最后再调用此方法句柄。在调用visitInvokeDynamicInsn方法时提供了一个额外的参数“Hello”。这个参数
   * 会被传递给bootstrap方法的最后一个参数value，用来创建目标方法句柄。当目标方法句柄被调用的时候，返回的结果是把
   * 参数“Hello”转换成大写形式之后的值“HELLO”
   */
  private static final Handle BSM = new Handle(
      Opcodes.H_INVOKESTATIC,
      ToUpperCase.class.getName().replace('.', '/'),
      "bootstrap",
      MethodType.methodType(CallSite.class, Lookup.class, String.class, MethodType.class, String.class).toMethodDescriptorString(),
      false);


  public static void main(String[] args) throws Exception {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    String packageName = InvokeDynamicInstructionTest.class.getPackage().getName().replace('.', '/');
    // 　注：一个编译后的java类不包含package和import段，因此在class文件中所有的类型都使用的是全路径。
    cw.visit(V1_8, ACC_PUBLIC | ACC_SUPER,  packageName + "/" +"ToUpperCaseMain", null, "java/lang/Object", null);

    MethodVisitor mv = cw.visitMethod(ACC_PUBLIC | ACC_STATIC, "main", "([Ljava/lang/String;)V", null, null);

    mv.visitCode();
    mv.visitFieldInsn(GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
    mv.visitInvokeDynamicInsn("toUpperCase", "()Ljava/lang/String;", BSM, "Hello");
    mv.visitMethodInsn(INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/String;)V");
    mv.visitInsn(RETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
    cw.visitEnd();

    Files.write(Paths.get(InvokeDynamicInstructionTest.class.getResource(".").getPath(),
        "ToUpperCaseMain.class"), cw.toByteArray());
  }

  public static class ToUpperCase {

    public static CallSite bootstrap(Lookup lookup, String name, MethodType type, String value) throws Exception {
      MethodHandle mh = lookup.findVirtual(String.class, "toUpperCase", MethodType.methodType(String.class))
          .bindTo(value);
      return new ConstantCallSite(mh);
    }
  }
}

