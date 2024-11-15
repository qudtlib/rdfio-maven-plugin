package io.github.qudtlib.maven.rdfio.product;

import java.util.ArrayList;
import java.util.List;
import org.apache.maven.plugins.annotations.Parameter;

public class Products {
    private final List<Product> products = new ArrayList<>();

    @Parameter
    public void setSingleFile(SingleFile singleFile) {
        this.products.add(singleFile);
    }

    @Parameter
    public void setEachFile(EachFile eachFile) {
        this.products.add(eachFile);
    }

    public List<Product> getProducts() {
        return products;
    }
}
