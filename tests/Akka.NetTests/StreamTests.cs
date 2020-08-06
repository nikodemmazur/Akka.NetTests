using Akka.TestKit;
using Xunit;
using System;
using Akka.Streams;
using Akka.Streams.Dsl;
using System.Threading.Tasks;
using Akka.Streams.Implementation.Fusing;
using System.Threading;
using System.Linq;
using System.Collections.Generic;
using Akka.Actor;
using FluentAssertions;
using Akka.Persistence;

namespace Akka.NetTests
{
    public class StreamTests : TestKit.Xunit2.TestKit
    {
        TimeSpan _epsilonValueForWithins;
        readonly ActorMaterializer _materializer;

        public StreamTests()
        {
            _materializer = Sys.Materializer();
            _epsilonValueForWithins = TimeSpan.FromSeconds(1);
        }

        [Fact]
        public void MaybeViaFluentApiMaterializedToBothWaitsForTcs()
        {
            Source<string, TaskCompletionSource<string>> source = Source.Maybe<string>();
            Sink<string, Task<string>> sink = Sink.First<string>();
            Aggregate<string, string> flow = new Aggregate<string, string>("Start, ", (agg, curr) => agg + curr);

            IRunnableGraph<(TaskCompletionSource<string>, Task<string>)> rg = source.Via(flow).ToMaterialized(sink, Keep.Both);

            (TaskCompletionSource<string> tcs, Task<string> task) = rg.Run(_materializer);

            Thread.Sleep(TimeSpan.FromMilliseconds(500));
            Assert.False(task.IsCompleted);
            tcs.SetResult("End!");
            task.Wait();
            Assert.True(task.IsCompleted);
            Assert.Equal("Start, End!", task.Result);
        }

        [Fact]
        public void RunnableGraphByGraphApiDoesntThrowEx()
        {
            IGraph<ClosedShape, NotUsed> gdsl = GraphDsl.Create(builder =>
            {
                Source<int, NotUsed> source = Source.From(Enumerable.Range(1, 100));
                var sink = Sink.Ignore<int>().MapMaterializedValue(_ => NotUsed.Instance);

                var broadcast = builder.Add(new Broadcast<int>(2));
                var merge = builder.Add(new Merge<int>(2));

                var f1 = Flow.Create<int>().Select(x => x + 10);
                var f2 = Flow.Create<int>().Select(x => x + 10);
                var f3 = Flow.Create<int>().Select(x => x + 10);
                var f4 = Flow.Create<int>().Select(x => x + 10);

                builder.From(source).Via(f1).Via(broadcast).Via(f2).Via(merge).Via(f3).To(sink);
                builder.From(broadcast).Via(f4).To(merge);

                return ClosedShape.Instance;
            });

            RunnableGraph<NotUsed> rg = RunnableGraph.FromGraph(gdsl);

            rg.Run(_materializer);
        }

        [Fact]
        public async Task RunnableGraphMadeOfBackpressuredQueueAndActorRefWithAckWorksAsExpected()
        {
            const int MAX = 4;

            Source<int, ISourceQueueWithComplete<int>> source = Source.Queue<int>(MAX, OverflowStrategy.Backpressure);
            TestProbe probe = CreateTestProbe();
            Sink<IEnumerable<int>, NotUsed> sink = Sink.ActorRefWithAck<IEnumerable<int>>(probe.Ref, "init", "ack", "complete");

            RunnableGraph<ISourceQueueWithComplete<int>> rg = RunnableGraph.FromGraph(GraphDsl.Create(source, sink, Keep.Left,
                (builder, source_, sink_) =>
                {
                    UniformFanOutShape<int, int> broadcaster = builder.Add(new Broadcast<int>(2));
                    UniformFanInShape<IEnumerable<int>, IEnumerable<int>> merger = builder.Add(new Merge<IEnumerable<int>>(2));

                    var f1 = Flow.Create<int>().Aggregate(new List<int>(),
                        (agg, curr) =>
                        {
                            agg.Add(curr);
                            return agg;
                        }).Select(list => list.AsEnumerable());
                    var f2 = Flow.Create<int>().Aggregate(new List<int>(),
                        (agg, curr) =>
                        {
                            agg.Add(curr);
                            return agg;
                        }).Select(list => list.AsEnumerable());

                    builder.From(source_).To(broadcaster.In);
                    builder.From(broadcaster.Out(0)).Via(f1).To(merger.In(0));
                    builder.From(broadcaster.Out(1)).Via(f2).To(merger.In(1));
                    builder.From(merger.Out).To(sink_);

                    return ClosedShape.Instance;
                }));

            ISourceQueueWithComplete<int> q = rg.Run(_materializer);

            probe.ExpectMsg<string>((msg, sender) =>
            {
                if (msg != "init")
                    throw new InvalidOperationException($"Expected: init. Found: {msg}");
                sender.Tell("ack");
            });
            await q.OfferAsync(2);
            await q.OfferAsync(4);
            await q.OfferAsync(8);
            await q.OfferAsync(16);
            q.Complete();
            await q.WatchCompletionAsync();

            probe.ExpectMsg<IEnumerable<int>>((msg, sender) =>
            {
                Assert.Equal(new[] { 2, 4, 8, 16 }.AsEnumerable(), msg);
                sender.Tell("ack");
            });
            probe.ExpectMsg<IEnumerable<int>>((msg, sender) =>
            {
                Assert.Equal(new[] { 2, 4, 8, 16 }.AsEnumerable(), msg);
                sender.Tell("ack");
            });

            probe.ExpectMsg("complete");
            probe.ExpectNoMsg();
        }

        [Fact]
        public async Task ElementsShouldBeDroppedAtDivideByZeroEx()
        {
            Streams.Supervision.Decider decider = 
                cause => cause is DivideByZeroException 
                    ? Streams.Supervision.Directive.Resume 
                    : Streams.Supervision.Directive.Stop;

            Flow<int, int, string> flow = Flow.Create<int>()
                .Where(x => 100 / x < 50)
                .Select(x => 100 / (5 - x))
                .MapMaterializedValue(_ => "materialization test")
                .WithAttributes(ActorAttributes.CreateSupervisionStrategy(decider));
            Source<int, float> source = Source.From(Enumerable.Range(0, 6))
                                              .MapMaterializedValue(_ => 2f); // Meaningless mapping just for test
            Sink<int, Task<int>> sink = Sink.Aggregate<int, int>(0, (sum, i) => sum + i);
            IRunnableGraph<(float, string)> materializationTestRunnableGraph = source.Via(flow).ViaMaterialized(flow, Keep.Both).To(sink);
            var rg = source.Via(flow).ToMaterialized(sink, Keep.Right);

            int result = await rg.Run(_materializer);

            Assert.Equal(150, result);
        }

        [Fact]
        public void StreamRecoversOneTimeWithDifferentSource()
        {
            var planB = Source.From(new List<string> { "five", "six", "seven", "eight" });

            Source.From(Enumerable.Range(0, 10)).Select(n =>
                {
                    if (n < 5)
                        return n.ToString();

                    throw new ArithmeticException("Boom!");
                })
                  .RecoverWithRetries(attempts: 1, partialFunc: exception => 
                      exception is ArithmeticException ? planB : null)
                  .RunForeach(Console.WriteLine, _materializer);
        }

        [Fact]
        public void PublishSubscribeOnHubsAddsAndRemovesPublishersAndSubscribers()
        {
            const int publisherMaxCount = 16;
            const int subscriberMaxCount = 16;
            const int bufferSize = 4;

            //       Source         ToMat          Bidi      
            //   +------------+               +------------+ 
            //   |  MergeHub  |               |BroadcastHub| 
            //   |   Source   | ~> Message ~> |    Sink    | 
            //   |            |               |            | 
            //   +------------+               +------------+ 
            //   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            //   ~~    (Sink<Message>, Source<Message>)   ~~
            //   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            (Sink<string, NotUsed> mergeSink, Source<string, NotUsed> mergeSource) =
                MergeHub.Source<string>(perProducerBufferSize: publisherMaxCount)
                        .ToMaterialized(BroadcastHub.Sink<string>(bufferSize: subscriberMaxCount), Keep.Both)
                        .Run(_materializer);

            TestProbe sub0 = CreateTestProbe();
            TestProbe sub1 = CreateTestProbe();

            //        Flow         JoinMat         Bidi                  
            //   +------------+               +------------+            
            //   |  FromSink  | ~> Message ~> |KillSwitches| ~> Message 
            //   |     And    |               |   Single   |            
            //   |    Source  | <~ Message <~ |    Bidi    | <~ Message 
            //   +------------+               +------------+            
            //   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            //   ~~                 UniqueKillSwitch                 ~~
            //   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            Flow<string, string, UniqueKillSwitch> busFlow =
                Flow.FromSinkAndSource(mergeSink, mergeSource)
                    .JoinMaterialized(KillSwitches.SingleBidi<string, string>(), Keep.Right);

            var (pub0, uniqueKillSwitch0) =
                Source.ActorRef<string>(bufferSize, OverflowStrategy.Fail)
                      .ViaMaterialized(busFlow, Keep.Both)
                      .To(Sink.ActorRef<string>(sub0, "complete"))
                      .Run(_materializer);

            pub0.Tell("It's chat member 0!");
            sub0.ExpectMsg("It's chat member 0!"); // Echo.
            sub0.ExpectNoMsg(TimeSpan.FromMilliseconds(50));

            var (pub1, uniqueKillSwitch1) =
                Source.ActorRef<string>(bufferSize, OverflowStrategy.Fail)
                    .ViaMaterialized(busFlow, Keep.Both)
                    .To(Sink.ActorRef<string>(sub1, "complete"))
                    .Run(_materializer);

            pub1.Tell("Hi! It's chat member 1!");
            sub1.ExpectMsg("Hi! It's chat member 1!"); // Echo.
            sub0.ExpectMsg("Hi! It's chat member 1!");
            pub0.Tell("Oh, Hi! Sry, but I gotta go, bye!");
            sub0.ExpectMsg("Oh, Hi! Sry, but I gotta go, bye!"); // Echo.
            uniqueKillSwitch0.Shutdown(); // Looks like this Shutdown is non-blocking.
            sub0.ExpectMsg("complete", 
                TimeSpan.FromMilliseconds(1000)); // Wait for the running graph to stop.
            sub1.ExpectMsg("Oh, Hi! Sry, but I gotta go, bye!");
            pub1.Tell("Oh, looks like I stayed alone.");
            sub1.ExpectMsg("Oh, looks like I stayed alone."); // Echo.
            sub0.ExpectNoMsg();
        }

        public class StreamInts
        {
            public StreamInts(Guid streamId)
            {
                StreamId = streamId;
            }

            public Guid StreamId { get; }
        }

        public class IntsStreamCreated
        {
            public IntsStreamCreated(Guid streamId, ISourceRef<int> sourceRef)
            {
                StreamId = streamId;
                SourceRef = sourceRef;
            }

            public Guid StreamId { get; }
            public ISourceRef<int> SourceRef { get; }
        }

        public class ElementEnqueued
        {
            private ElementEnqueued() { }
            static ElementEnqueued()
            {
                Instance = new ElementEnqueued();
            }
            public static ElementEnqueued Instance { get; }
        }

        public class RequestLogs
        {
            public long StreamId { get; }
            public RequestLogs(long streamId)
            {
                StreamId = streamId;
            }
        }

        public class LogsOffer
        {
            public long StreamId { get; }
            public ISourceRef<string> SourceRef { get; }
            public LogsOffer(long streamId, ISourceRef<string> sourceRef)
            {
                StreamId = streamId;
                SourceRef = sourceRef;
            }
        }

        public class LogsSourceActor : ReceiveActor
        {
            public LogsSourceActor()
            {
                Receive<RequestLogs>(req =>
                {
                    var streamLogs = Source.From(Enumerable.Range(1, 5)).Select(i => i.ToString());
                    streamLogs.ToMaterialized(StreamRefs.SourceRef<string>(), Keep.Right)
                              .Run(Context.System.Materializer())
                              .PipeTo(Sender, success: sourceRef => new LogsOffer(req.StreamId, sourceRef));
                });
            }
        }

        [Fact]
        public async Task TestSysReadsFromAnotherSystemViaStream()
        {
            var anotherSystem = ActorSystem.Create("AnotherSystem");
            var sourceActor = anotherSystem.ActorOf<LogsSourceActor>("LogsSourceActor");

            var streamId = Convert.ToInt64(new Random().Next());
            var offer = await sourceActor.Ask<LogsOffer>(new RequestLogs(streamId));

            var actual = 
                await offer
                    .SourceRef
                    .Source
                    .Via(Flow.Create<string>().Grouped(5))
                    .ToMaterialized(Sink.First<IEnumerable<string>>(), Keep.Right)
                    .Run(Sys.Materializer());

            Assert.Equal(new[] { "1", "2", "3", "4", "5" }, actual);
        }

        public class PrepareUpload
        {
            public Guid StreamId { get; }
            public PrepareUpload(Guid streamId)
            {
                StreamId = streamId;
            }
        }

        public class MeasurementsSinkReady
        {
            public Guid StreamId { get; }
            public ISinkRef<double> SinkRef { get; }
            public MeasurementsSinkReady(Guid streamId, ISinkRef<double> sinkRef)
            {
                StreamId = streamId;
                SinkRef = sinkRef;
            }
        }

        public class StreamCompleted 
        {
            public static StreamCompleted Instance { get; }
            private StreamCompleted() { }
            static StreamCompleted()
            {
                Instance = new StreamCompleted();
            }
        }

        public class MeasurementsTargetActor : ReceiveActor
        {
            private double _avg;
            private readonly Sink<double, Task> _avgSink;
            private readonly IActorRef _thisActorRef;
            private readonly IActorRef _forwardActorRef;
            private Task? _streamTask;

            public MeasurementsTargetActor(int windowSize, IActorRef forwardActorRef)
            {
                _forwardActorRef = forwardActorRef;
                _thisActorRef = Context.Self;
                _avgSink =
                    Flow
                        .Create<double>()
                        .WatchTermination(Keep.Right)
                        .Sliding(windowSize)
                        .Collect(win => 
                            win.Average())
                        .AlsoTo(Sink.ForEach<double>(avg =>
                            _forwardActorRef.Tell(avg)))
                        .To(Sink.ForEach<double>(avg => 
                            _thisActorRef.Tell(avg))); // Calling Self here, uses Context of the stream instead of the actor!

                Receive<PrepareUpload>(pu =>
                {
                    var t = StreamRefs
                        .SinkRef<double>()
                        .ToMaterialized(_avgSink, Keep.Both)
                        .Run(Context.System.Materializer());

                    t.Item1.PipeTo(Sender, success: sr => new MeasurementsSinkReady(pu.StreamId, sr));
                    _streamTask = t.Item2;
                });

                Receive<double>(d =>
                {
                    _avg = d;
                });

                Receive<string>(str => str == "notify when stream completes", delegate
                {
                    _streamTask.PipeTo(Sender, success: () => StreamCompleted.Instance);
                });

                Receive<string>(str => str == "get avg", delegate
                {
                    Sender.Tell(_avg);
                });
            }
        }

        [Fact]
        public async Task TestSysWritesToAnotherSystemViaStream()
        {
            const int windowSize = 3;

            var anotherSystem = ActorSystem.Create("AnotherSystem");
            var sinkActorProps = Props.Create<MeasurementsTargetActor>(windowSize, TestActor);
            var sinkActor = anotherSystem.ActorOf(sinkActorProps, "MeasurementsSinkActor");

            Guid streamId = Guid.NewGuid();
            var ready = await sinkActor.Ask<MeasurementsSinkReady>(new PrepareUpload(streamId));
            Assert.Equal(streamId, ready.StreamId);

            var sourceTask = Source
                .From(Enumerable.Range(1, 4))
                .Select(i => Convert.ToDouble(i))
                .WatchTermination(Keep.Right)
                .To(ready.SinkRef.Sink)
                .Run(Sys.Materializer()); // Notice that SinkRef is materialized on the Origin-side as well.

            sourceTask.Wait();
            Assert.True(sourceTask.IsCompletedSuccessfully);
            await sinkActor.Ask<StreamCompleted>("notify when stream completes");

            ExpectMsg<double>(d => d == (1D + 2D + 3D) / 3D);
            ExpectMsg<double>(d => d == (2D + 3D + 4D) / 3D);
        }

        [Fact]
        public async Task StreamRefThrowsSubscriptionTimeout()
        {
            var ex = await Assert.ThrowsAsync<RemoteStreamRefActorTerminatedException>(async delegate
            {
                TimeSpan subscriptionTimeout = TimeSpan.FromSeconds(3);

                var sourceRef = await Source
                    .Single("NeverConsumedMsg")
                    .RunWith(StreamRefs.SourceRef<string>()
                        .AddAttributes(StreamRefAttributes.CreateSubscriptionTimeout(subscriptionTimeout)),
                        Sys.Materializer());

                Task.Delay(subscriptionTimeout + TimeSpan.FromSeconds(1)).Wait();

                await sourceRef.Source.RunWith(Sink.Ignore<string>(), Sys.Materializer());
            });

            // ex.InnerException.Should().BeOfType<StreamRefSubscriptionTimeoutException>(); // null inside
        }
    }
}