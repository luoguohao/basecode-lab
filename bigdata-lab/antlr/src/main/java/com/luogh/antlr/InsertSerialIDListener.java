package com.luogh.antlr;

import com.luogh.antlr.Java8Parser.ClassBodyContext;
import lombok.Getter;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.TokenStreamRewriter;

public class InsertSerialIDListener extends com.luogh.antlr.Java8BaseListener {

  @Getter
  private TokenStreamRewriter rewriter;

  public InsertSerialIDListener(TokenStream commonTokenStream) {
    this.rewriter = new TokenStreamRewriter(commonTokenStream);
  }

  @Override
  public void enterClassBody(ClassBodyContext ctx) {
    String field = "\n\tpublic static final long serialVersionUID = 1L;\n";
    rewriter.insertAfter(ctx.start, field);
  }
}
