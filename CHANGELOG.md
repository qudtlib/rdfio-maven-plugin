# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.6.2] - 2025-09-01
### Fixed
- Rename <shaclValidate>/<failForMissingInputGraph> to <shaclValidate>/<failOnMissingInputGraph>
- Add <shaclInfer>/<failForMissingInputGraph> parameter

## [1.6.1] - 2025-09-01
### Fixed
- Fixed <shaclValidate>/<failForMissingInputGraph>: correctly use 'true' as default value

## [1.6.0] - 2025-09-01
### Added
- New optional parameter <shaclValidate>/<failForMissingInputGraph> - allow input graphs to be empty in the pipeline dataset

### Fixed
- Improve formatting of validation report

## [1.5.5] - 2025-07-18
### Fixed
- filter validation results by `logSeverity` in `<shaclValidate>` console output

## [1.5.4] - 2025-07-16
### Fixed
- Copy namespace prefixes from all models in a dataset to a model when writing it in pipeline step <write>

## [1.5.3] - 2025-07-15
### Fixed
- fix ${project.basedir} being baked in at build time

## [1.5.2] - 2025-07-15
### Fixed
- fix decimal.pow() - return 0 for value 0 and negative exponent

## [1.5.1] - 2025-07-11
### Fixed
- fixed plugin.xml

## [1.5.0] - 2025-07-11
### Added
- a few custom sparql functions for decimals:
- division https://github.com/qudtlib/numericFunctions/decimal.div
- power https://github.com/qudtlib/numericFunctions/decimal.pow
- precision https://github.com/qudtlib/numericFunctions/decimal.precision
- roundToPrecision https://github.com/qudtlib/numericFunctions/decimal.roundToPrecision
- <when> step
- <stop> step

### Changed
- changed the URI prefix for resources used in pipeline metadata to https://github.com/qudtlib/rdfio/
- <clear> now supports <message>, <graphs> and <graph> subelements to support clearing specified graphs, not the whole dataset

## [1.4.5] - 2025-07-09
### Added
- Add `<message>` to `<write>` step

## [1.4.4] - 2025-07-08
### Fixed
- `<add>` multiple files to default graph no longer dies
- step hash now includes file content for InputsComponent (if filenames/paths don't contain variables, which are only known at pipeline execution time)

###

## [1.4.3] - 2025-06-30
### Fixed
- Filename handling in metadata graph of pipeline mojo

## [1.4.2] - 2025-06-24
### Added
- Pipeline Mojo

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

[Unreleased]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.6.2...HEAD
[1.6.2]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.6.1...v1.6.2
[1.6.1]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.6.0...v1.6.1
[1.6.0]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.5.5...v1.6.0
[1.5.5]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.5.4...v1.5.5
[1.5.4]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.5.3...v1.5.4
[1.5.3]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.5.2...v1.5.3
[1.5.2]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.5.1...v1.5.2
[1.5.1]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.5.0...v1.5.1
[1.5.0]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.4.5...v1.5.0
[1.4.5]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.4.4...v1.4.5
[1.4.4]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.4.3...v1.4.4
[1.4.3]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.4.2...v1.4.3
[1.4.2]: https://github.com/qudtlib/rdfio-maven-plugin/compare/v1.4.1...v1.4.2
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
