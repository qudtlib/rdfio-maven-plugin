# RDFIO Maven Plugin

The RDFIO Maven Plugin is a versatile tool for processing RDF data within Maven builds, leveraging Apache Jena, SHACL, and SPARQL. It provides two primary goals: `make` for combining and filtering RDF files, and `pipeline` for defining complex RDF processing workflows. This plugin is ideal for tasks such as data transformation, validation, inference, and output generation in RDF-based projects.

## Installation

Add the plugin to your `pom.xml`:

```xml
<plugin>
    <groupId>io.github.qudtlib</groupId>
    <artifactId>rdfio-maven-plugin</artifactId>
    <version>[current version]</version>
</plugin>
```

## Goals

The plugin supports two main goals: `make` and `pipeline`. Each goal serves distinct purposes and is configured separately.

### Make Goal

The `make` goal, bound to the `generate-sources` phase by default, processes RDF files to produce output files. It supports two product types: `singleFile` for combining multiple inputs into one output, and `eachFile` for processing multiple inputs individually. The `make` goal allows filtering and transformation of RDF data using various filter types.

#### Make: SingleFile

The `singleFile` product combines multiple RDF input files into a single output file, optionally applying filters to transform the data.

- **Configuration**:
  - `<input>`: Required. Specifies input files or graphs via `<file>`, `<files>`, or `<graph>`.
    - `<include>`: Ant-style patterns for files to include (e.g., `**/*.ttl`).
    - `<exclude>`: Patterns for files to exclude.
    - `<graph>`: Graph URI for the input data.
  - `<outputFile>`: Required. The path to the output file (e.g., `target/combined.ttl`).
  - `<graph>`: Optional. Graph URIs to process. If omitted, uses the default graph.
  - `<filters>`: Optional. Filters to apply to the dataset (see [Filters](#filters)).
  - `<skip>`: Optional. If `true`, skips processing. Default: `false`.

- **Example**:
  ```xml
  <execution>
      <id>combine-rdf-files</id>
      <phase>generate-sources</phase>
      <goals>
          <goal>make</goal>
      </goals>
      <configuration>
          <products>
              <singleFile>
                  <input>
                      <include>src/main/resources/rdf/*.ttl</include>
                      <exclude>**/temp/*.ttl</exclude>
                  </input>
                  <outputFile>target/combined.ttl</outputFile>
                  <filters>
                      <exclude>
                          <predicate>rdfs:comment</predicate>
                      </exclude>
                      <sparqlUpdate>
                          DELETE WHERE { ?s rdf:type ex:WrongType }
                      </sparqlUpdate>
                  </filters>
              </singleFile>
          </products>
      </configuration>
  </execution>
  ```
  This example combines all `.ttl` files in `src/main/resources/rdf`, excludes temporary files, removes triples with `rdfs:comment` predicates or `ex:WrongType` types, and writes the result to `target/combined.ttl`.

#### Make: EachFile

The `eachFile` product processes multiple RDF input files individually, producing one output file per input, either overwriting the input files or writing to a specified output directory.

- **Configuration**:
  - `<input>`: Required. Specifies input files or graphs via `<file>`, `<files>`, or `<graph>`.
    - `<include>`: Ant-style patterns for files to include.
    - `<exclude>`: Patterns for files to exclude.
    - `<graph>`: Graph URI for the input data.
  - `<outputDir>`: Optional. Directory for output files (e.g., `target/output`). Ignored if `replaceInputFiles` is `true`.
  - `<graph>`: Optional. Graph URIs to process. If omitted, uses the default graph.
  - `<filters>`: Optional. Filters to apply to each dataset (see [Filters](#filters)).
  - `<replaceInputFiles>`: Optional. If `true`, overwrites input files. Default: `false`.
  - `<skip>`: Optional. If `true`, skips processing. Default: `false`.

- **Example**:
  ```xml
  <execution>
      <id>process-each-rdf-file</id>
      <phase>generate-sources</phase>
      <goals>
          <goal>make</goal>
      </goals>
      <configuration>
          <products>
              <eachFile>
                  <input>
                      <include>src/main/resources/rdf/*.ttl</include>
                  </input>
                  <outputDir>target/processed</outputDir>
                  <filters>
                      <exclude>
                          <predicate>rdfs:label</predicate>
                      </exclude>
                  </filters>
              </eachFile>
          </products>
      </configuration>
  </execution>
  ```
  This example processes each `.ttl` file in `src/main/resources/rdf`, removes triples with `rdfs:label` predicates, and writes the results to `target/processed`.

#### Filters

Filters transform RDF data in `singleFile` and `eachFile` products. They are applied in the order specified and can be one of the following:

- **Exclude**: Removes triples matching specified predicates.
  - `<predicate>`: Predicate URI (e.g., `rdfs:comment`).
- **Include**: Retains only triples matching specified predicates.
  - `<predicate>`: Predicate URI.
- **SparqlUpdate**: Executes a SPARQL Update query to modify the dataset.
  - Inline query or via `<sparqlUpdateFile>` with a file path.
- **SparqlSelect**: Executes a SPARQL SELECT query, logging results.
  - Inline query or via `<sparqlSelectFile>` with a file path.
- **SparqlConstruct**: Executes a SPARQL CONSTRUCT query, adding results to the dataset.
  - Inline query or via `<sparqlConstructFile>` with a file path.
- **GraphUnion**: Combines multiple graphs into one.
  - `<graph>`: Graph URIs to union.
  - `<outputGraph>`: Target graph for the result.
- **GraphMinus**: Removes triples from one graph that appear in others.
  - `<graph>`: Graph URIs to process.
  - `<outputGraph>`: Target graph for the result.

- **Example**:
  ```xml
  <filters>
      <exclude>
          <predicate>rdfs:comment</predicate>
      </exclude>
      <sparqlUpdate>
          DELETE WHERE { ?s rdf:type ex:WrongType }
      </sparqlUpdate>
      <sparqlSelect>
          SELECT * WHERE { ?s ?p ?o }
      </sparqlSelect>
  </filters>
  ```

### Pipeline Goal

The `pipeline` goal defines a sequence of steps to manipulate an in-memory RDF dataset. It supports complex workflows, including data loading, SPARQL operations, SHACL inference and validation, iterative processing, and output generation.

#### Pipeline Configuration

The pipeline is configured under the `<pipeline>` element:

- **Attributes**:
  - `<id>`: Required. Unique pipeline identifier.
  - `<metadataGraph>`: Optional. Graph URI for metadata (e.g., file-to-graph mappings). Default: `rdfio:pipeline:metadata`.
  - `<forceRun>`: Optional. If `true`, ignores savepoints. Default: `false`.
  - `<baseDir>`: Optional. Base directory for file operations. Default: `${project.basedir}`.
  - `<steps>`: Required. List of pipeline steps.

#### Pipeline Steps

1. **AddStep**
   Loads RDF data into the dataset.

- **Configuration**:
  - `<file>` or `<files>`: Input files.
  - `<graph>` or `<graphs>`: Input graphs.
  - `<toGraph>`: Target graph URI.
  - `<toGraphsPattern>`: Pattern for target graphs (e.g., `test:${name}`), using `${path}`, `${name}`, or `${index}`.

- **Example**:
  ```xml
  <add>
      <file>src/data.ttl</file>
      <toGraph>test:data</toGraph>
  </add>
  ```

2. **ForeachStep**
   Iterates over graphs, executing nested steps.

- **Configuration**:
  - `<var>`: Variable name for the current graph URI.
  - `<values>`: Graph selection via `<graphs>` with `<include>` and `<exclude>`.
  - `<body>`: Nested steps.

- **Example**:
  ```xml
  <foreach>
      <var>graph</var>
      <values><graphs><include>test:*</include></graphs></values>
      <body>
          <sparqlUpdate>
              <sparql>INSERT DATA { GRAPH ?graph { <http://example.org/s> <http://example.org/p> "test" } }</sparql>
          </sparqlUpdate>
      </body>
  </foreach>
  ```

3. **UntilStep**
   Loops until a SPARQL ASK query returns `true`.

- **Configuration**:
  - `<sparqlAsk>`: SPARQL ASK query.
  - `<indexVar>`: Variable for iteration index.
  - `<message>`: Log message.
  - `<body>`: Nested steps.

- **Example**:
  ```xml
  <until>
      <sparqlAsk>ASK { ?s ?p ?o FILTER NOT EXISTS { ?s ex:processed "true" } }</sparqlAsk>
      <body>
          <sparqlUpdate>
              <sparql>INSERT { ?s ex:processed "true" } WHERE { ?s ?p ?o }</sparql>
          </sparqlUpdate>
      </body>
  </until>
  ```

4. **WriteStep**
   Writes RDF data to files.

- **Configuration**:
  - `<graph>`: Graph URIs to write.
  - `<toFile>`: Output file path. If omitted, writes to associated files.

- **Example**:
  ```xml
  <write>
      <graph>test:data</graph>
      <toFile>target/output.ttl</toFile>
  </write>
  ```

5. **ShaclInferStep**
   Performs SHACL-based inference.

- **Configuration**:
  - `<message>`: Description.
  - `<shapes>`: SHACL shapes via `<file>`, `<files>`, `<graph>`, or `<graphs>`.
  - `<data>`: Data sources.
  - `<inferred>`: Output via `<graph>` and/or `<file>`.
  - `<iterateUntilStable>`: Repeat until no new triples.
  - `<iterationOutputFilePattern>`: Per-iteration output pattern.

- **Example**:
  ```xml
  <shaclInfer>
      <shapes><file>shapes.ttl</file></shapes>
      <data><graph>test:data</graph></data>
      <inferred><graph>inferred:graph</graph></inferred>
  </shaclInfer>
  ```

6. **ShaclValidateStep**
   Validates RDF data against SHACL shapes.

- **Configuration**:
  - `<message>`: Description.
  - `<shapes>`: SHACL shapes.
  - `<data>`: Data sources.
  - `<validationReport>`: Output via `<graph>` and/or `<file>`.
  - `<failOnSeverity>`: Severity to fail build (Info, Warning, Violation, None).
  - `<logSeverity>`: Minimum severity to log.

- **Example**:
  ```xml
  <shaclValidate>
      <shapes><graph>shapes:graph</graph></shapes>
      <data><graph>test:data</graph></data>
      <validationReport><file>report.ttl</file></validationReport>
  </shaclValidate>
  ```

7. **SparqlQueryStep**
   Executes SPARQL queries (ASK, SELECT, CONSTRUCT, DESCRIBE).

- **Configuration**:
  - `<sparql>` or `<file>`: Query.
  - `<message>`: Log message.
  - `<toFile>`: Result file.
  - `<toGraph>`: Result graph for CONSTRUCT/DESCRIBE.

- **Example**:
  ```xml
  <sparqlQuery>
      <sparql>SELECT ?s ?p ?o WHERE { ?s ?p ?o }</sparql>
      <toFile>results.txt</toFile>
  </sparqlQuery>
  ```

8. **SparqlUpdateStep**
   Executes SPARQL Update operations.

- **Configuration**:
  - `<sparql>` or `<file>`: Update query.
  - `<message>`: Log message.

- **Example**:
  ```xml
  <sparqlUpdate>
      <sparql>INSERT DATA { GRAPH <test:graph> { <http://example.org/s> <http://example.org/p> "test" } }</sparql>
  </sparqlUpdate>
  ```

9. **SavepointStep**
   Caches dataset state for optimization.

- **Configuration**:
  - `<id>`: Savepoint identifier.
  - `<enabled>`: Enable/disable savepoint.

- **Example**:
  ```xml
  <savepoint>
      <id>sp001</id>
  </savepoint>
  ```

10. **ShaclFunctionsStep**
    Registers SHACL SPARQL functions.

- **Configuration**:
  - `<file>`, `<files>`, `<graph>`, or `<graphs>`: Function definitions.

- **Example**:
  ```xml
  <shaclFunctions>
      <file>shacl-functions.ttl</file>
  </shaclFunctions>
  ```

#### Variable Resolution

Uses `${variable}` syntax, resolved from the metadata graph.

- **Example**:
  ```xml
  <write>
      <toFile>output-${graphName}.ttl</toFile>
  </write>
  ```

#### Graph and File Selection

- **GraphSelection**: `<include>` and `<exclude>` patterns (e.g., `test:*`).
- **FileSelection**: Ant-style patterns (e.g., `**/*.ttl`).

## SHACL Function Support

SHACL SPARQL functions can be registered via `<importShaclFunctions>` in `make` or `<shaclFunctions>` in `pipeline`.

- **Example**:
  ```xml
  <products>
      <importShaclFunctions>
          <include>shacl-functions.ttl</include>
      </importShaclFunctions>
  </products>
  ```

## Example Configuration

```xml
<plugin>
    <groupId>io.github.qudtlib</groupId>
    <artifactId>rdfio-maven-plugin</artifactId>
    <version>1.0.0</version>
    <executions>
        <execution>
            <id>make-and-pipeline</id>
            <phase>generate-sources</phase>
            <goals>
                <goal>make</goal>
                <goal>pipeline</goal>
            </goals>
            <configuration>
                <products>
                    <singleFile>
                        <input>
                            <include>src/main/resources/raw/*.ttl</include>
                        </input>
                        <outputFile>target/preprocessed.ttl</outputFile>
                        <filters>
                            <exclude>
                                <predicate>rdfs:comment</predicate>
                            </exclude>
                        </filters>
                    </singleFile>
                </products>
                <pipeline>
                    <id>rdf-processing</id>
                    <steps>
                        <add>
                            <file>target/preprocessed.ttl</file>
                            <toGraph>test:raw</toGraph>
                        </add>
                        <shaclInfer>
                            <shapes><file>shapes.ttl</file></shapes>
                            <data><graph>test:raw</graph></data>
                            <inferred><graph>inferred:graph</graph></inferred>
                        </shaclInfer>
                        <write>
                            <graph>inferred:graph</graph>
                            <toFile>target/final.ttl</toFile>
                        </write>
                    </steps>
                </pipeline>
            </configuration>
        </execution>
    </executions>
</plugin>
```

## Contributing

Submit pull requests or issues on the [GitHub repository](https://github.com/qudtlib/rdfio-maven-plugin).

## License

Apache License 2.0.