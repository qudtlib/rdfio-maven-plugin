package io.github.qudtlib.maven.rdfio.common.sparql.function;

import io.github.qudtlib.maven.rdfio.common.NF;
import java.math.BigDecimal;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionBase1;

public class DecimalPrecision extends FunctionBase1 {

    public static String getUri() {
        return NF.decimal_precision.toString();
    }

    @Override
    public NodeValue exec(NodeValue value) {
        if (!value.isNumber()) {
            throw new IllegalArgumentException(
                    "First argument ('value') to df:decimal.precision(value:number) is not numeric: "
                            + value);
        }
        BigDecimal bdValue = value.getDecimal();
        return NodeValue.makeInteger(bdValue.precision());
    }
}
