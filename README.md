## Media Types and Interfaces for Janelia Services [![Picture](https://raw.github.com/janelia-flyem/janelia-flyem.github.com/master/images/gray_janelia_logo.png)](http://janelia.org/)

This repository holds descriptions of each non-standard Media Type (formerly known as MIME types) and REST interface specifications supported by Janelia services. 

### Contents

This repository contains files describing media types and REST APIs.

Different media types are defined
with a *{schema}.json* in JSON schema format.  There are also *{schema}.example.json* that gives an example
implementation of the corresonding schema.

REST APIs for each service should be specified in RAML format.

This repo will be globally accessible at: http://janelia-flyem.github.io/service-contracts/.


### Defining a custom media type

Please consult examples in this repository and follow instructions on how to create
a JSON schema at [json-schema.org](http://www.json-schema.org).  JSON schema allows
one to specify the validation and format of a JSON file.

The media type for JSONs that follow this schema should be **application/schema+json**

JSON schema is language neutral and there are several tools out there that will validate
a JSON file against the supplied schema.  JSON schema can also transform JSON into
hypertext by supporting URI links.

An example of an online schema validator:
[example](http://json-schema-validator.herokuapp.com/).

Guidelines for using JSON schema in FlyEM:

* JSON schema files should have the following naming convention {service-name}-{keyword}-v{x.xx}.schema.json.  In some cases, a schema will be generic to many services and {service-name} can be ommitted.
* Each JSON schema and all of its versions should be in a separate directory.
* JSON schema should contain the version number of the schema.  The name of the file should match this version number.  Old versions of a JSON schema should remain in the repo.  Minor version changes such as 0.31 to 0.32 indicate backward compliance.  Version changes from 0.3 to 0.4 indicate incompatibility.
* The current JSON schema should have an example (replace 'schema' with 'example' in the name).
* JSON schema should have a mandatory "version" property.  If a client sends JSON with an old version in a property field, the server should notify the client that it is using a deprecated version.

### Defining custom RAML

Web services should define a RESTful API in [RAML](http://raml.org).  Please consult examples, such as [serviceproxy](https://github.com/janelia-flyem/serviceproxy).

Guidelines for using RAML interfaces in FlyEM:

* RAML should have the following naming conventions {service-name}v{x.xx}.raml.
* Each RAML and all of its version should be in a separate directory.
* RAML versioning should follow the conventions defined for JSON schema.
* Services should return the RAML interface using the URI /interface
* Services should return just the RAML version using the URI /interface/version.
* RAML should reference the JSON schema rather than embed the whole file when stored in this repo (it is assumed that http://janelia-flyem.github.io/service-contracts/<json schema name> is a legitimate reference).  However, it is encouraged that servers that return RAML at /interface embed the JSON for the client.
