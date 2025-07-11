package io.github.qudtlib.maven.rdfio.common.sparql.function;

import io.github.qudtlib.maven.rdfio.common.NF;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionBase2;

public class DecimalRoundToPrecision extends FunctionBase2 {

    public static String getUri() {
        return NF.decimal_roundToPrecision.toString();
    }

    @Override
    public NodeValue exec(NodeValue value, NodeValue precision) {
        if (!value.isNumber()) {
            throw new IllegalArgumentException(
                    "First argument ('value') to df:decimal.roundToPrecision(value:number, precision:integer) is not numeric: "
                            + value);
        }
        if (!precision.isInteger()) {
            throw new IllegalArgumentException(
                    "Second argument ('precision') to df:decimal.roundToPrecision(value:number, precision:integer) is not an integer: "
                            + value);
        }
        BigDecimal bdValue = value.getDecimal();
        MathContext mc =
                new MathContext(precision.getInteger().intValueExact(), RoundingMode.HALF_UP);
        return NodeValue.makeDecimal(bdValue.round(mc));
    }
}
