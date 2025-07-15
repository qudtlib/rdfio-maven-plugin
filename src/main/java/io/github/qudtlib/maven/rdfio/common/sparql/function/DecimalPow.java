package io.github.qudtlib.maven.rdfio.common.sparql.function;

import io.github.qudtlib.maven.rdfio.common.NF;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionBase3;

public class DecimalPow extends FunctionBase3 {

    public static String getUri() {
        return NF.decimal_pow.toString();
    }

    @Override
    public NodeValue exec(NodeValue value, NodeValue exponent, NodeValue precision) {
        if (!value.isNumber()) {
            throw new IllegalArgumentException(
                    "First argument ('value') to decimal.pow(value:number, exponent:integer, precision:integer) is not numeric: "
                            + value);
        }
        if (!exponent.isInteger()) {
            throw new IllegalArgumentException(
                    "Second argument ('exponent') to decimal.pow(value:number, exponent:integer, precision:integer) is not an integer: "
                            + exponent);
        }
        if (!precision.isInteger()) {
            throw new IllegalArgumentException(
                    "Third argument ('precision') to decimal.pow(value:number, exponent:integer, precision:integer) is not an integer: "
                            + value);
        }
        BigDecimal bdValue = value.getDecimal();
        BigInteger biExponent = exponent.getInteger();
        BigInteger biPrecision = precision.getInteger();
        MathContext mc = new MathContext(biPrecision.intValueExact());
        int intExponent = biExponent.intValueExact();
        if (intExponent < 0) {
            if (bdValue.compareTo(BigDecimal.ZERO) == 0) {
                return NodeValue.makeDecimal(BigDecimal.ZERO);
            }
        }
        BigDecimal result = bdValue.pow(intExponent, mc);
        return NodeValue.makeDecimal(result);
    }
}
