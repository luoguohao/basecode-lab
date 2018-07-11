package com.luogh.antlr;


import java.util.Arrays;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.Assert;
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

  @Test
  public void testEmbeddingArbitraryCode() throws Exception {
    int col = 1;
    CharStream charStream = CharStreams.fromStream(ClassLoader.getSystemResourceAsStream("rows.testdata"));
    com.luogh.antlr.RowsLexer lexer = new com.luogh.antlr.RowsLexer(charStream);
    CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
    com.luogh.antlr.RowsParser parser = new com.luogh.antlr.RowsParser(commonTokenStream, col); // parse column number
    parser.setBuildParseTree(false); // don't waste time building a tree
    parser.file();
  }

  /**
   * 语义谓词，即语法树的解析依赖谓词判断
   * 本例中通过对一组数据：1 10 2 20 20 3 1 2 3
   * 按照每一次解析的数据个数，将数据进行分组：
   * 即：1表示将取10一个数据分组
   *    2表示将取2个数：20 20 作为同一组
   *    3表示取3个数：1 2 3 作为同一组
   * 结果如下
   * (file (group 1 (sequence 10)) (group 2 (sequence 20 20)) (group 3 (sequence 1 2 3)))
   * @throws Exception
   */
  @Test
  public void testSemanticPredicate() throws Exception {
    CharStream charStream = CharStreams.fromStream(ClassLoader.getSystemResourceAsStream("data.testdata"));
    com.luogh.antlr.DataLexer lexer = new com.luogh.antlr.DataLexer(charStream);
    CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
    com.luogh.antlr.DataParser parser = new com.luogh.antlr.DataParser(commonTokenStream);
    System.out.println(parser.file().toStringTree(parser));
  }

  /**
   * lexer feature: lexical modes syntax.
   * switch different mode when lexer sees special sentinel character sequences
   */
  @Test
  public void testLexicalModelSyntax() throws Exception {
    CharStream charStream = CharStreams.fromStream(ClassLoader.getSystemResourceAsStream("xml.testdata"));
    com.luogh.antlr.XMLLexer lexer = new com.luogh.antlr.XMLLexer(charStream);
    CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
    commonTokenStream.fill();
    /*
     * token format:
     * [@{TokenIndex},{start character index}: {stop character index}='{token text}',<{tokenType}>,{line index}:{character position within the line}]
     *
     * example: [@0,0:0='<',<1>,1:0]
     */
    commonTokenStream.getTokens().stream().forEach(System.out::println);
  }

  /**
   * lexer feature: rewrite the input stream
   */
  @Test
  public void testRewritingInputStreamUsingTokenStreamReWriter() throws Exception {
    CharStream charStream = CharStreams.fromStream(ClassLoader.getSystemResourceAsStream("java8.testdata"));
    com.luogh.antlr.Java8Lexer lexer = new com.luogh.antlr.Java8Lexer(charStream);
    CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
    com.luogh.antlr.Java8Parser parser = new com.luogh.antlr.Java8Parser(commonTokenStream);
    ParseTreeWalker walker = new ParseTreeWalker(); // create standard walker
    InsertSerialIDListener extractor = new InsertSerialIDListener(commonTokenStream);
    walker.walk(extractor, parser.compilationUnit()); // initiate walk of tree with listener
    System.out.println(extractor.getRewriter().getText());
  }

  /**
   * lexer feature: sending tokens on different channels
   * there are two channel :
   *  1. default channel(id=0)
   *  2. hidden channel (id=1)
   */
  @Test
  public void testTokenChannel() throws Exception {
    CharStream charStream = CharStreams.fromStream(ClassLoader.getSystemResourceAsStream("channel.testdata"));
    com.luogh.antlr.ChannelExprLexer lexer = new com.luogh.antlr.ChannelExprLexer(charStream);
    CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
    commonTokenStream.fill();
    Assert.assertArrayEquals(new String[]{"DEFAULT_TOKEN_CHANNEL", "HIDDEN"}, lexer.getChannelNames());
  }
}
