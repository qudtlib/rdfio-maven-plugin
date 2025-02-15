# RDF file manipulation for maven builds

## Targets
 
`make`: makes RDF files. For now, all you can do is combine multiple 
RDF files using the `input` section of a `singleFile` element, as shown 
in the example below, filter triples and use SPARQL Update queries to manipulate data.

RDF format of input and output files is determined by their file name extensions.

How filters work:
* filter types: 
  - `<exclude>` and `<include>`: each can have multiple `<predicate>` entries, which filters triples by predicate
  - `<sparqlUdpate>`: allows for specifying an inline SPARQL Update query. 
    Bonus: Prefixes occurring in the data do not need to be added to the query.  
  - `<sparqlUpdateFile>`: allows for specifying a project file to read the SPARQL Update query from. Same Bonus.
  - `<sparqlConstruct>`: allows for specifying an inline SPARQL Construct query. Constructed triples are added to the data.
  - `<sparqlConstructFile>`: allows for specifying a SPARQL Construct query from a project file. Constructed triples are added to the data.
* multiple filter elements are allowed
* each filter is applied to the whole RDF graph.
* all filters are applied in the order they appear in the configuration

 

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
                            <!-- change the model with an inline SPARQL update -->
                            <sparqlUpdate>
                                DELETE WHERE {?any rdf:type ex:WrongType }
                            </sparqlUpdate>
                            <!-- change the model with a SPARQL update read from a project file -->
                            <sparqlUpdateFile>src/main/resources/sparql/update1.rq</sparqlUpdateFile>
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
