package dev.trailbeam.sqs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

/** An SQS client over a canned HTTP layer: each request is answered by {@code handler} (JSON protocol). */
final class StubSqs implements SdkHttpClient {
  static final String URL = "http://localhost:9324/000000000000/orders";

  /** One recorded request: the operation (from X-Amz-Target), its JSON body and its headers. */
  static final class Req {
    final String operation;
    final String body;
    final java.util.Map<String, List<String>> headers;

    Req(String operation, String body, java.util.Map<String, List<String>> headers) {
      this.operation = operation;
      this.body = body;
      this.headers = headers;
    }
  }

  /** A canned answer: a status and a JSON body, or an I/O error to throw. */
  static final class Res {
    final int status;
    final String body;
    final String queryError;
    final IOException thrown;

    private Res(int status, String body, String queryError, IOException thrown) {
      this.status = status;
      this.body = body;
      this.queryError = queryError;
      this.thrown = thrown;
    }

    static Res ok(String json) {
      return new Res(200, json, null, null);
    }

    static Res error(String type, String queryError) {
      return new Res(400, "{\"__type\":\"" + type + "\",\"message\":\"stub error\"}", queryError, null);
    }

    static Res thrown(IOException e) {
      return new Res(0, null, null, e);
    }
  }

  final List<Req> requests = new ArrayList<>();
  private final Function<Req, Res> handler;

  StubSqs(Function<Req, Res> handler) {
    this.handler = handler;
  }

  SqsClient client(ExecutionInterceptor... extra) {
    SqsClientBuilder b = SqsClient.builder()
        .httpClient(this)
        .region(Region.US_EAST_1)
        .endpointOverride(URI.create("http://localhost:9324"))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
        .overrideConfiguration(c -> {
          c.retryStrategy(AwsRetryStrategy.doNotRetry());
          for (ExecutionInterceptor i : extra) {
            c.addExecutionInterceptor(i);
          }
        });
    return b.build();
  }

  @Override
  public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
    return new ExecutableHttpRequest() {
      @Override
      public HttpExecuteResponse call() throws IOException {
        String body = request.contentStreamProvider().isPresent()
            ? new String(request.contentStreamProvider().get().newStream().readAllBytes(), StandardCharsets.UTF_8)
            : "";
        String target = request.httpRequest().firstMatchingHeader("X-Amz-Target").orElse("");
        Req r = new Req(target.substring(target.indexOf('.') + 1), body, request.httpRequest().headers());
        synchronized (requests) {
          requests.add(r);
        }
        Res res = handler.apply(r);
        if (res.thrown != null) {
          throw res.thrown;
        }
        SdkHttpResponse.Builder h = SdkHttpResponse.builder().statusCode(res.status)
            .putHeader("Content-Type", "application/x-amz-json-1.0");
        if (res.queryError != null) {
          h.putHeader("x-amzn-query-error", res.queryError);
        }
        byte[] bytes = res.body.getBytes(StandardCharsets.UTF_8);
        h.putHeader("Content-Length", String.valueOf(bytes.length));
        return HttpExecuteResponse.builder().response(h.build())
            .responseBody(AbortableInputStream.create(new ByteArrayInputStream(bytes))).build();
      }

      @Override
      public void abort() {}
    };
  }

  @Override
  public void close() {}

  static String md5(String s) {
    try {
      byte[] d = MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      for (byte x : d) {
        sb.append(String.format("%02x", x));
      }
      return sb.toString();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  static String json(String s) {
    return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
