# RDF file manipulation for maven builds

## Targets
 
`make`: makes RDF files. For now, all you can do is combine multiple 
RDF files using the `input` section of a `singleFile` element, as shown 
in the example below.

RDF format of input and output files is determined by their file name extensions.

How filters work:
* filter/include keeps all triples that contain one of the listed predicates
* filter/exclude keeps all triples that do not contain any of the listed predicates
* multiple include and exclude elements are allowed, each is applied to the whole RDF
graph. They are applied in sequence.

 

## Example

```xml
<build> 
  <plugins>
    <plugin>
      <groupId>io.github.qudtlib</groupId>
      <artifactId>rdfio-maven-plugin</artifactId>
      <version>1.0.0</version>
      <executions>
        <execution>
          <id>combine files a and b</id>
          <phase>compile</phase>
          <goals>
            <goal>make</goal>
          </goals>
            <configuration>
                <products>
                    <singleFile>
                        <input>
                            <include>**/*.ttl</include>
                        </input>
                        <filters>
                            <!-- filters are executed in order. Prefixes that occor in the input can be used -->
                            <exclude>
                                <predicate>rdfs:comment</predicate>
                            </exclude>
                            <sparqlUpdate>
                                DELETE WHERE {?any rdf:type ex:WrongType }
                            </sparqlUpdate>
                            <exclude>
                                <predicate>rdfs:label</predicate>
                            </exclude>
                        </filters>
                        <outputFile>target/combined.ttl</outputFile>
                    </singleFile>
                    <eachFile>
                        <replaceInputFiles>true</replaceInputFiles>
                        <input>
                            <include>**/*.ttl</include>
                        </input>
                        <filters>
                            <!-- filters are executed in order. Prefixes that occor in the input can be used -->
                            <exclude>
                                <predicate>rdfs:comment</predicate>
                            </exclude>
                            <sparqlUpdate>
                                DELETE WHERE {?any rdf:type ex:WrongType }
                            </sparqlUpdate>
                            <exclude>
                                <predicate>rdfs:label</predicate>
                            </exclude>
                        </filters>
                    </eachFile>
                    <eachFile>
                        <replaceInputFiles>false</replaceInputFiles>
                        <outputDir>target/myfiles</outputDir>
                        <input>
                            <include>**/*.ttl</include>
                        </input>
                        <filters>
                            <!-- filters are executed in order. Prefixes that occor in the input can be used -->
                            <exclude>
                                <predicate>rdfs:comment</predicate>
                            </exclude>
                            <sparqlUpdate>
                                DELETE WHERE {?any rdf:type ex:WrongType }
                            </sparqlUpdate>
                            <exclude>
                                <predicate>rdfs:label</predicate>
                            </exclude>
                        </filters>
                    </eachFile>
                </products>
            </configuration>
        </execution>
      </executions>
    </plugin>
  </plugins>
</build>
```
