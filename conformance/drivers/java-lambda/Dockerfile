# The Java conformance function: the AWS Lambda base image (with its Runtime Interface Emulator), the jar built
# from this checkout's java/ (tests are not in the context), and LambdaHandler, a RequestHandler<SQSEvent,
# SQSBatchResponse> that knows nothing of Trailbeam. The function's handler is the Trailbeam wrapper, with the real
# handler in TRAILBEAM_LAMBDA_HANDLER, as java/README.md tells users. The poller builds it from a context with
# java/ and this directory (driver.json, "--context").
FROM maven:3.9.9-eclipse-temurin-21 AS build
COPY java /build/java
RUN mvn -B -ntp -q -f /build/java/pom.xml install -Dmaven.test.skip=true
COPY conformance/drivers/java-lambda /build/fn
RUN mvn -B -ntp -q -f /build/fn/pom.xml package \
 && echo "trailbeam-aws-sdk-v2 $(cat /build/java/target/maven-archiver/pom.properties | sed -n 's/^version=//p')" > /build/kit-versions.txt

FROM public.ecr.aws/lambda/java:21
COPY --from=build /build/fn/target/classes ${LAMBDA_TASK_ROOT}
COPY --from=build /build/fn/target/lib ${LAMBDA_TASK_ROOT}/lib/
COPY --from=build /build/kit-versions.txt /kit-versions.txt
ENV TRAILBEAM_LAMBDA_HANDLER=trailbeam.kit.LambdaHandler
CMD ["dev.trailbeam.sqs.TrailbeamSqsLambdaWrapper::handleRequest"]
