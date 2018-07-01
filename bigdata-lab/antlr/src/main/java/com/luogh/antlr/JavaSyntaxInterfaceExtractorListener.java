package com.luogh.antlr;

import com.luogh.antlr.Java8Parser.FormalParameterContext;
import com.luogh.antlr.Java8Parser.MethodDeclarationContext;
import com.luogh.antlr.Java8Parser.MethodDeclaratorContext;
import com.luogh.antlr.Java8Parser.NormalClassDeclarationContext;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class JavaSyntaxInterfaceExtractorListener extends com.luogh.antlr.Java8BaseListener {

  private com.luogh.antlr.Java8Parser tree;

  public JavaSyntaxInterfaceExtractorListener(com.luogh.antlr.Java8Parser tree) {
    this.tree = tree;
  }


  @Override
  public void enterNormalClassDeclaration(NormalClassDeclarationContext ctx) {
    System.out.println("interface" + " " + "I" + ctx.Identifier().getText() + " {");
  }

  @Override
  public void exitNormalClassDeclaration(NormalClassDeclarationContext ctx) {
    System.out.println("}");
  }

  /**
   * methodHeader :	result methodDeclarator throws_? |	typeParameters annotation* result
   * methodDeclarator throws_? ;
   */
  @Override
  public void enterMethodDeclaration(MethodDeclarationContext ctx) {
    String result = ctx.methodHeader().result().getText();
    String throwsStr = "";
    if (ctx.methodHeader().throws_() != null) {
      throwsStr = ctx.methodHeader().throws_().getText();
    }
    String typePrameterString = "";
    ParseTree typeParameters = ctx.methodHeader().typeParameters();
    if (typeParameters != null) {
      typePrameterString = typeParameters.getText();
    }
    MethodDeclaratorContext methodDeclarator = ctx.methodHeader().methodDeclarator();
    String identifier = methodDeclarator.Identifier().getText();
    TokenStream tokens = tree.getTokenStream();
    String parameters = methodDeclarator.formalParameterList().getText();
    System.out.println(
        typePrameterString + " " + result + " " + identifier + " (" + parameters + " )" + throwsStr
            + ";");
  }


  @Override
  public void exitFormalParameter(FormalParameterContext ctx) {
    super.exitFormalParameter(ctx);
  }
}
