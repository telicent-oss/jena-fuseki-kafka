### Deploy with maven

Make sure you have authorized with AWS CodeArtifact (valid for 12 hours):

```
export CODEARTIFACT_AUTH_TOKEN=`aws codeartifact get-authorization-token --domain telicent --domain-owner 098669589541 --query authorizationToken --output text`
```

([Documentation](https://eu-west-2.console.aws.amazon.com/codesuite/codeartifact/d/098669589541/telicent/r/telicent-code-artifacts?packages-meta=eyJmIjp7fSwicyI6e30sIm4iOjIwLCJpIjowfQ&region=eu-west-2#).)

Push:
```
   mvn deploy
```

Release:

see [README#release](./README.md#release).

Setup:

```
   source release-setup
```


Check the version numbers.

Dry run 

```
mvn $MVN_ARGS -DdryRun=true release:clean release:prepare
```

and for real

```
mvn $MVN_ARGS release:clean release:prepare
mvn $MVN_ARGS release:perform
```