# RDF file manipulation for maven builds

## Targets
 
`combine`: 

 

## Example

Example configuring  [build lifecycle phases](https://maven.apache.org/guides/introduction/introduction-to-the-lifecycle.html):

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
