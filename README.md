# RDF file manipulation for maven builds

## Targets
 
`make`: makes RDF files. For now, all you can do is combine multiple 
RDF files using the `input` section of a `singleFile` element, as shown 
in the example below.

RDF format of input and output files is determined by their file name extensions.

 

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
                        <outputFile>target/combined.ttl</outputFile>
                    </singleFile>
                </products>
            </configuration>
        </execution>
    </plugin>
  </plugins>
</build>
```
