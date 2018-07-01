package com.luogh.antlr;


import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.Test;

public class AntlrRunner {

  @Test
  public void testAntlr() throws Exception {
    // create a CharStream that reads from String
    CharStream in = CharStreams.fromString("(a+b)*2");
    // create a lexer that feeds off of input CharStream
    com.luogh.antlr.ExprLexer lexer = new com.luogh.antlr.ExprLexer(in);
    // create a buffer of tokens pulled from the lexer
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    // create a parser that feeds off the tokens buffer
    com.luogh.antlr.ExprParser parser = new com.luogh.antlr.ExprParser(tokens);
    ParseTree context = parser.prog();  // begin parsing at prog rule
    System.out.println(context.toStringTree(parser)); // print LISP-style tree
  }


  @Test
  public void testAntlrListener() throws Exception {
    // create a CharStream that reads from String
    CharStream in = CharStreams.fromString("{1,2,4,{1,2,4},4}");
    // create a lexer that feeds off of input CharStream
    com.luogh.antlr.ArrayInitLexer lexer = new com.luogh.antlr.ArrayInitLexer(in);
    // create a buffer of tokens pulled from the lexer
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    // create a parser that feeds off the tokens buffer
    com.luogh.antlr.ArrayInitParser parser = new com.luogh.antlr.ArrayInitParser(tokens);
    ParseTree context = parser.init();  // begin parsing at init rule
    System.out.println(context.toStringTree(parser)); // print LISP-style tree

    ParseTreeWalker walker = new ParseTreeWalker();
    walker.walk(new ArrayInitConvertListener(), context);
  }

  @Test
  public void testExprPro() throws Exception {
    CharStream charStream = CharStreams.fromStream(ClassLoader.getSystemResourceAsStream("exprPro.testdata"));
    com.luogh.antlr.ExprProLexer lexer = new com.luogh.antlr.ExprProLexer(charStream);
    CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
    com.luogh.antlr.ExprProParser parser = new com.luogh.antlr.ExprProParser(commonTokenStream);
    ParseTree tree = parser.prog();
    System.out.println(tree.toStringTree(parser));
  }

  @Test
  public void testLabeledExpr() throws Exception {
    CharStream charStream = CharStreams.fromStream(ClassLoader.getSystemResourceAsStream("exprPro.testdata"));
    com.luogh.antlr.LabeledExprLexer lexer = new com.luogh.antlr.LabeledExprLexer(charStream);
    CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
    com.luogh.antlr.LabeledExprParser parser = new com.luogh.antlr.LabeledExprParser(commonTokenStream);
    ParseTree tree = parser.prog();
    LabeledExprCalculatorVisitor visitor = new LabeledExprCalculatorVisitor();
    visitor.visit(tree);
  }

  @Test
  public void testJava8Expr() throws Exception {
    CharStream charStream = CharStreams.fromStream(ClassLoader.getSystemResourceAsStream("java8.testdata"));
    com.luogh.antlr.Java8Lexer lexer = new com.luogh.antlr.Java8Lexer(charStream);
    CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
    com.luogh.antlr.Java8Parser parser = new com.luogh.antlr.Java8Parser(commonTokenStream);
    ParseTree tree = parser.compilationUnit();
    ParseTreeWalker walker = new ParseTreeWalker();
    walker.walk(new JavaSyntaxInterfaceExtractorListener(parser), tree);
  }
}
