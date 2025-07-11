package io.github.qudtlib.maven.rdfio.common.sparql.function;

import io.github.qudtlib.maven.rdfio.common.NF;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionBase3;

public class DecimalDiv extends FunctionBase3 {

    public static String getUri() {
        return NF.decimal_div.toString();
    }

    @Override
    public NodeValue exec(NodeValue value, NodeValue divisor, NodeValue precision) {
        if (!value.isNumber()) {
            throw new IllegalArgumentException(
                    "First argument ('value') to df:decimal.div(value:number, divisor:number, precision:integer) is not numeric: "
                            + value);
        }
        if (!divisor.isNumber()) {
            throw new IllegalArgumentException(
                    "Second argument ('divisor') to df:decimal.div(value:number, divisor:number, precision:integer) is not numeric: "
                            + divisor);
        }
        if (!precision.isInteger()) {
            throw new IllegalArgumentException(
                    "Third argument ('precision') to df:decimal.div(value:number, divisor:number, precision:integer) is not an integer: "
                            + value);
        }
        BigDecimal bdValue = value.getDecimal();
        BigInteger biPrecision = precision.getInteger();
        MathContext mc = new MathContext(biPrecision.intValueExact());
        BigDecimal result = bdValue.divide(divisor.getDecimal(), mc);
        return NodeValue.makeDecimal(result);
    }
}
