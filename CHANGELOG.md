# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.4.1] - 2025-06-23

## [1.4.0] - 2025-06-23

## [1.3.2] - 2025-03-12

## [1.3.1] - 2025-02-25

## [1.3.0] - 2025-02-15
### Added
- Add `<sparqlUpdateFile>` filter so the sparql udpate query can be read from a project file
- Add `<sparqlConstruct>` filter so a sparql construct query can be used to generate additional triples
- Add `<sparqlConstructFile>` filter so the sparql construct query can be read from a project file

## [1.2.1] - 2024-11-15
### Fixed
- Fixed bug only observed in some build environments that caused the SparqlUpdateFilter failing to be instantiated.

## [1.2.0] - 2024-11-15
### Added
- EachFile product type, allowing for a set of input files to be processed individually and optionally changed in-place.
- SparqlUpdate filter, allowing for deleting and adding triples based on a sparql update query.

## [1.1.2] - 2024-11-08

## [1.1.1] - 2024-11-08
### Fixed
- Fixed bug which caused multiline strings to be written with a '\r' appended to each line.

## [1.1.0] - 2024-11-05
### Added
- Allow filtering of RDF content

## 1.0.0 - 2024-11-03
### Added
- Initial version of the plugin offering just making a single RDF file as the combination of multiple files.

[Unreleased]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.4.1...HEAD
[1.4.1]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.4.0...v1.4.1
[1.4.0]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.3.2...v1.4.0
[1.3.2]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.3.1...v1.3.2
[1.3.1]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.2.1...v1.3.0
[1.2.1]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.1.2...v1.2.0
[1.1.2]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.0.0...v1.1.0
