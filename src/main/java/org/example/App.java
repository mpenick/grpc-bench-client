package org.example;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import io.grpc.CallCredentials;
import io.grpc.CallCredentials.MetadataApplier;
import io.grpc.CallCredentials.RequestInfo;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Payload.Type;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Values;
import io.stargate.proto.StargateGrpc;
import io.stargate.proto.StargateGrpc.StargateFutureStub;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.sql.Driver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import jnr.ffi.annotations.In;
import org.example.proto.GreeterGrpc;
import org.example.proto.GreeterGrpc.GreeterFutureStub;
import org.example.proto.GreeterGrpc.GreeterStub;
import org.example.proto.Service.HelloReply;
import org.example.proto.Service.HelloRequest;

/**
 * Hello world!
 */
public class App {

  private static final int  numThreads = 64;
  private static final MetricRegistry metrics = new MetricRegistry();
  private static final Timer requests = metrics.timer("requests");
  private static final ExecutorService executor = Executors.newFixedThreadPool(numThreads);

  public static void main(String[] args) throws InterruptedException {
    ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
    reporter.start(2, TimeUnit.SECONDS);
    //runGrpc();
    //runDriver();
    //runGrpcSimple();
    runGrpcSimpleAgain();
  }

  public static void runDriver() {

    CqlSession session = CqlSession.builder()
        .withLocalDatacenter("dc1")
        .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
        .withConfigLoader(DriverConfigLoader.programmaticBuilder()
        .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 1)
            .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, 1)
            .build())
        .build();

    for (int i = 0; i < numThreads; ++i) {
      executor.submit(() -> {
        while (true) {
          try (Timer.Context ignored = requests.time()) {
            try {
              session.execute("select * from baselines.keyvalue where key=?", "3299266");
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      });
    }
  }

  public static void runGrpc() throws InterruptedException {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("127.0.0.1", 8090)
            .usePlaintext()
            .directExecutor()
            .build();

    String token = "cbd3c2f0-3d11-4b30-8673-9e03e9310e86";
    StargateFutureStub stub = StargateGrpc.newFutureStub(channel).withCallCredentials(new StargateBearerToken(token));
    //GreeterFutureStub stub = GreeterGrpc.newFutureStub(channel);

    QueryParameters parameters = QueryParameters.newBuilder().setSkipMetadata(true).build();
    for (int i = 0; i < numThreads; ++i) {
      executor.submit(() -> {
        while (true) {
          try (Timer.Context ignored = requests.time()) {
            try {
              Any data = Any.pack(Values.newBuilder().addValues(Value.newBuilder().setString("3299266")).build());
              ListenableFuture<Response> future = stub.executeQuery(Query.newBuilder().setCql(
                  "select * from baselines.keyvalue where key=?")
                  .setParameters(parameters)
                  .setValues(Payload.newBuilder().setType(Type.CQL).setData(data))
                  .build());
              //ListenableFuture<HelloReply> future = stub
              //    .sayHello(HelloRequest.newBuilder().setName("Michael").build());
              future.get();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      });
    }

    while (!executor.awaitTermination(9999, TimeUnit.DAYS)) {
    }
  }

  public static void runGrpcSimple() throws InterruptedException {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("127.0.0.1", 8090)
            .usePlaintext()
            .directExecutor()
            .build();

    GreeterFutureStub stub = GreeterGrpc.newFutureStub(channel);

    for (int i = 0; i < numThreads; ++i) {
      executor.submit(() -> {
        while (true) {
          try (Timer.Context ignored = requests.time()) {
            try {
              ListenableFuture<HelloReply> future = stub
                  .sayHello(HelloRequest.newBuilder().setName("Michael").build());
              future.get();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      });
    }

    while (!executor.awaitTermination(9999, TimeUnit.DAYS)) {
    }
  }

  public static void runGrpcSimpleAgain() throws InterruptedException {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("127.0.0.1", 8090)
            .usePlaintext()
            .directExecutor()
            .build();

    GreeterStub stub = GreeterGrpc.newStub(channel);

    for (int i = 0; i < numThreads; ++i) {
      Semaphore semaphore = new Semaphore(1);
      executor.submit(() -> {
        StreamObserver<HelloRequest> observer = stub.sayHelloAgain(new StreamObserver<HelloReply>() {
          @Override
          public void onNext(HelloReply value) {
            semaphore.release();
          }

          @Override
          public void onError(Throwable t) {
            throw new RuntimeException(t);
          }

          @Override
          public void onCompleted() {
          }
        });

        while (true) {
          try (Timer.Context ignored = requests.time()) {
            try {
              semaphore.acquire();
              observer.onNext(HelloRequest.newBuilder().setName("Michael").build());
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      });
    }

    while (!executor.awaitTermination(9999, TimeUnit.DAYS)) {
    }
  }

  public static class StargateBearerToken extends CallCredentials {
    public static final Metadata.Key<String> TOKEN_KEY =
        Metadata.Key.of("X-Cassandra-Token", Metadata.ASCII_STRING_MARSHALLER);

    private final String token;

    public StargateBearerToken(String token) {
      this.token = token;
    }

    @Override
    public void applyRequestMetadata(
        RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
      appExecutor.execute(
          () -> {
            try {
              Metadata metadata = new Metadata();
              metadata.put(TOKEN_KEY, token);
              applier.apply(metadata);
            } catch (Exception e) {
              applier.fail(Status.UNAUTHENTICATED.withCause(e));
            }
          });
    }

    @Override
    public void thisUsesUnstableApi() {}
  }
}
