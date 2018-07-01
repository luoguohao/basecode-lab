package com.luogh.antlr;

import static com.luogh.antlr.LabeledExprParser.ADD;
import static com.luogh.antlr.LabeledExprParser.DIV;
import static com.luogh.antlr.LabeledExprParser.MUL;
import static com.luogh.antlr.LabeledExprParser.SUB;

import com.google.common.base.Preconditions;
import com.luogh.antlr.LabeledExprParser.AssignContext;
import com.luogh.antlr.LabeledExprParser.BlankContext;
import com.luogh.antlr.LabeledExprParser.IdContext;
import com.luogh.antlr.LabeledExprParser.IntContext;
import com.luogh.antlr.LabeledExprParser.OperationContext;
import com.luogh.antlr.LabeledExprParser.ParensContext;
import com.luogh.antlr.LabeledExprParser.PrintExprContext;
import java.util.HashMap;
import java.util.Map;

public class LabeledExprCalculatorVisitor extends com.luogh.antlr.LabeledExprBaseVisitor<Integer> {

  private final Map<String, Integer> memory = new HashMap<>();

  /** expr NEWLINE    # printExpr */
  @Override
  public Integer visitPrintExpr(PrintExprContext ctx) {
    Integer num = visit(ctx.expr());
    System.out.println(num);
    return 0;
  }

  /** ID '=' expr NEWLINE # assign*/
  @Override
  public Integer visitAssign(AssignContext ctx) {
    String id = ctx.ID().getText();
    Integer value = visit(ctx.expr());
    memory.put(id, value);
    return value;
  }

  /**NEWLINE  # blank */
  @Override
  public Integer visitBlank(BlankContext ctx) {
    return 0;
  }


  /** '(' expr  ')' # parens*/
  @Override
  public Integer visitParens(ParensContext ctx) {
    return visit(ctx.expr());
  }


  /**ID  # id*/
  @Override
  public Integer visitId(IdContext ctx) {
    String id = ctx.getText();
    if (memory.containsKey(id)) {
      return memory.get(id);
    } else {
      throw new IllegalArgumentException("not found valid parameter:" + id);
    }
  }


  /**expr (MUL|DIV|ADD|SUB) expr # operation */
  @Override
  public Integer visitOperation(OperationContext ctx) {
    Preconditions.checkArgument(ctx.children.size() == 3);
    Integer leftV = visit(ctx.expr(0));
    Integer rightV = visit(ctx.expr(1));
    int result ;
    switch (ctx.opt.getType()) {
      case MUL:
        result = leftV * rightV;
        break;
      case DIV:
        Preconditions.checkArgument(rightV != 0);
        result = leftV / rightV;
        break;
      case ADD:
        result = leftV + rightV;
        break;
      case SUB:
        result = leftV - rightV;
        break;
        default:
          throw new IllegalArgumentException("invalid operator:" + ctx.opt.getText());
    }
    return result;
  }


  /**INT  # int*/
  @Override
  public Integer visitInt(IntContext ctx) {
    return Integer.parseInt(ctx.INT().getText());
  }
}
