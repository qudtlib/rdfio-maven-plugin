package io.github.qudtlib.maven.rdfio.product;

import io.github.qudtlib.maven.rdfio.common.file.FileSelection;
import java.util.ArrayList;
import java.util.List;
import org.apache.maven.plugins.annotations.Parameter;

public class Products {
    private final List<Product> products = new ArrayList<>();
    private FileSelection importShaclFunctions = null;

    @Parameter
    public void setSingleFile(SingleFile singleFile) {
        this.products.add(singleFile);
    }

    @Parameter
    public void setEachFile(EachFile eachFile) {
        this.products.add(eachFile);
    }

    @Parameter
    public void importShaclFunctions(FileSelection fileSelection) {
        this.importShaclFunctions = fileSelection;
    }

    public List<Product> getProducts() {
        return products;
    }

    public FileSelection getImportShaclFunctions() {
        return this.importShaclFunctions;
    }
}
