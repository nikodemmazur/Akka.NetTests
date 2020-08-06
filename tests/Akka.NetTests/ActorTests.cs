using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Serialization;
using Akka.Actor;
using Akka.Configuration;
using Akka.Dispatch;
using Akka.Event;
using Akka.IO;
using Akka.Persistence;
using Akka.Routing;
using Akka.Streams.Implementation.Fusing;
using Akka.TestKit;
using FluentAssertions;
using Microsoft.VisualBasic;
using Xunit;

namespace Akka.NetTests
{
    public class ActorTests : TestKit.Xunit2.TestKit
    {
        public sealed class VariableBehaviorActor : ReceiveActor
        {
            public VariableBehaviorActor()
            {
                Receive<string>(_ =>
                {
                    Sender.Tell("Ctor behavior."); // Sends the sender implicitly.
                    Become(SecondBehavior); // Never call Become directly from ctor. It has no effects then.
                });
            }

            void SecondBehavior()
            {
                Receive<string>(_ =>
                {
                    Sender.Tell("Other behavior.");
                });
            }
        }

        public sealed class IntenseComputationActor : ReceiveActor
        {
            public IntenseComputationActor()
            {
                Receive<string>(_ =>
                {
                    Thread.Sleep(1000);
                    Sender.Tell("Intense data.", Self);
                });
            }
        }

        public sealed class EchoActor : UntypedActor
        {
            protected override void OnReceive(object message) =>
                Sender.Tell(message, Self);
        }

        public sealed class SelfStoppingEchoActor : UntypedActor
        {
            protected override void OnReceive(object message)
            {
                Sender.Tell(message, Self);
                Become(Failing);
            }

            void Failing(object message)
            {
                try
                {
                    throw new InvalidOperationException();
                }
                catch
                {
                    Context.Stop(Self); // Finishes processing of current msg, then terminates.
                }
            }
        }

        public sealed class FailingActor : UntypedActor
        {
            protected override void OnReceive(object message) =>
                throw new NotImplementedException();
        }

        public sealed class FrozenActor : UntypedActor
        {
            protected override void OnReceive(object message)
            {
                if (message as string != "Freeze!")
                    Sender.Tell(message, Self);
                else
                {
                    var cts = new CancellationTokenSource();
                    Sender.Tell(cts, Self);
                    try
                    {
                        Task.Delay((int)TimeSpan.FromMinutes(7).TotalMilliseconds, cts.Token)
                            .Wait();
                    }
                    catch
                    {
                        Context.Stop(Self);
                    }
                }
            }
        }

        public sealed class StashingActor : UntypedActor, IWithUnboundedStash
        {
            protected override void OnReceive(object message)
            {
                if ((message as string)?.Contains("stash", StringComparison.OrdinalIgnoreCase) ?? false)
                    Stash!.Stash();
                else
                    Sender.Tell(message, Self);
            }

            public IStash? Stash { get; set; }
        }

        public sealed class ForwardingActor : UntypedActor
        {
            private readonly IActorRef _recipient;

            public ForwardingActor(IActorRef recipient)
            {
                _recipient = recipient;
            }

            protected override void OnReceive(object message)
            {
                _recipient.Tell(message);
            }
        }

        public sealed class SupervisorActor : UntypedActor
        {
            private IActorRef? _friend;

            protected override SupervisorStrategy SupervisorStrategy()
            {
                return new OneForOneStrategy(5,
                    TimeSpan.FromMinutes(1),
                    ex => ex is NullReferenceException
                        ? Directive.Escalate
                        : Directive.Resume);
            }

            protected override void OnReceive(object message)
            {
                switch (message)
                {
                    case Props p:
                        {
                            var child = Context.ActorOf(p);
                            Sender.Tell(child);
                            break;
                        }
                    case IActorRef a:
                        _friend = a;
                        break;
                }
            }

            protected override void PreRestart(Exception reason, object message)
            {
                _friend?.Tell("I'm gonna commit seppuku.");
                Self.Tell(PoisonPill.Instance);
                // base.PreRestart(reason, message); - Don't shutdown. Orphan children.
            }
        }

        public sealed class SupervisedActor : UntypedActor
        {
            private string _state = string.Empty;

            protected override void OnReceive(object message)
            {
                switch (message)
                {
                    case Exception ex:
                        throw ex;
                    case "get":
                        Sender.Tell(_state);
                        break;
                    case string str:
                        _state = str;
                        break;
                }
            }
        }

        [Fact]
        public async Task ActorInitialBehaviorComesFromCtor()
        {
            Props autProps = Props.Create<VariableBehaviorActor>();
            IActorRef aut = Sys.ActorOf(autProps);

            string answer = await aut.Ask<string>(string.Empty, TimeSpan.FromMilliseconds(100));
            Assert.Contains("ctor", answer, StringComparison.OrdinalIgnoreCase);

            answer = await aut.Ask<string>(string.Empty, TimeSpan.FromMilliseconds(100));
            Assert.Contains("other", answer, StringComparison.OrdinalIgnoreCase);

            answer = await aut.Ask<string>(string.Empty, TimeSpan.FromMilliseconds(100));
            Assert.Contains("other", answer, StringComparison.OrdinalIgnoreCase);

            ExpectNoMsg(TimeSpan.FromMilliseconds(250));
        }

        [Fact]
        public void TestKitWatchesDeath()
        {
            var notSoTalkativeActor = Sys.ActorOf(Props.Empty);

            Watch(notSoTalkativeActor);

            ExpectNoMsg(TimeSpan.FromMilliseconds(150));

            notSoTalkativeActor.Tell(PoisonPill.Instance);

            ExpectMsg<Terminated>(msg => msg.ActorRef.Equals(notSoTalkativeActor));

            Unwatch(notSoTalkativeActor);
        }

        [Fact]
        public void DeathWatchGetsTerminatedMsgOnActorKill()
        {
            var aut = Sys.ActorOf(Props.Empty);

            Watch(aut);

            aut.Tell(Kill.Instance, ActorRefs.NoSender);

            ExpectTerminated(aut);

            Unwatch(aut);
        }

        [Fact]
        public void TestKitWaitsLongEnoughForMsg()
        {
            var longAnsweringActor = Sys.ActorOf<IntenseComputationActor>();

            longAnsweringActor.Tell(string.Empty);

            ExpectMsg<string>("Intense data.");
        }

        [Fact]
        public async Task ActorStopsRespondingAfterBeingKilled()
        {
            var aut = Sys.ActorOf<EchoActor>();

            var result = await aut.Ask<string>("Echo.", TimeSpan.FromMilliseconds(50));

            Assert.Equal("Echo.", result);

            Watch(aut);

            aut.Tell(Kill.Instance, ActorRefs.NoSender);

            ExpectTerminated(aut); // Wait for kill.

            aut.Tell("Second echo.");

            ExpectNoMsg(TimeSpan.FromMilliseconds(1000));

            Unwatch(aut);
        }

        [Fact]
        public async Task NonAkkaTestAwaitCollectsAggregateExOnCancellation()
        {
            var cts = new CancellationTokenSource();

            var task = LongRunningMethod(cts.Token);

            cts.CancelAfter(500);

            var ex = await Assert.ThrowsAsync<AggregateException>(() => task);

            Assert.Contains(ex.Flatten().InnerExceptions, ex_ => ex_ is TaskCanceledException);

            static Task<bool> LongRunningMethod(CancellationToken cancellationToken)
            {
                var tcs = new TaskCompletionSource<bool>();

                Task.Run(() =>
                {
                    try
                    {
                        Task.WaitAll(Task.Delay(1000, cancellationToken),
                            Task.Run(() => throw new OperationCanceledException()));
                    }
                    catch (Exception e)
                    {
                        tcs.SetException(e);
                    }
                    tcs.SetResult(true);
                });

                return tcs.Task;
            }
        }

        [Fact]
        public async Task ActorStopsRespondingAfterBeingPoisoned()
        {
            var aut = Sys.ActorOf<EchoActor>();

            var result = await aut.Ask<string>("Echo.", TimeSpan.FromMilliseconds(50));

            Assert.Equal("Echo.", result);

            Watch(aut);

            aut.Tell(PoisonPill.Instance, ActorRefs.NoSender);

            ExpectMsg<Terminated>(); // Wait for kill.

            aut.Tell("Second echo.");

            ExpectNoMsg(TimeSpan.FromMilliseconds(1000));

            Unwatch(aut);
        }

        [Fact]
        public async Task ActorStopsRespondingAfterThrowingEx()
        {
            var aut = Sys.ActorOf<SelfStoppingEchoActor>();

            var result = await aut.Ask<string>("Echo.", TimeSpan.FromMilliseconds(50));

            Assert.Equal("Echo.", result);

            await Assert.ThrowsAsync<AskTimeoutException>(() =>
                aut.Ask<string>("Echo.", TimeSpan.FromMilliseconds(100)));

            await Assert.ThrowsAsync<AskTimeoutException>(() =>
                aut.Ask<string>("Echo.", TimeSpan.FromMilliseconds(100)));
        }

        [Fact]
        public void UserChildRestartsByDefault()
        {
            var aut = Sys.ActorOf<FailingActor>();

            Watch(aut);

            aut.Tell(string.Empty);
            aut.Tell(string.Empty);
            aut.Tell(string.Empty);

            ExpectNoMsg(TimeSpan.FromMilliseconds(500)); // No Terminated message.

            Unwatch(aut);
        }

        [Fact]
        public void MessageBecomesDeadLetterAfterActorFail()
        {
            var failingActor = Sys.ActorOf<SelfStoppingEchoActor>();
            var probe = CreateTestProbe();

            Sys.EventStream.Subscribe(probe.Ref, typeof(DeadLetter));

            failingActor.Tell("Non-dead letter.");
            ExpectMsg("Non-dead letter.");

            failingActor.Tell("This msg will stop you!");

            failingActor.Tell("Dead letter.");

            probe.ExpectMsg<DeadLetter>(dl =>
                (string)dl.Message == "Dead letter.");

            Sys.EventStream.Unsubscribe(probe.Ref);
        }

        [Fact]
        public void StashBecomesDeadLettersWhenActorIsKilled()
        {
            var aut = Sys.ActorOf<StashingActor>();
            var probe = CreateTestProbe();

            Sys.EventStream.Subscribe(probe.Ref, typeof(DeadLetter));

            aut.Tell("Hi, stash this message.");
            aut.Tell("Stash this as well.");

            ExpectNoMsg(TimeSpan.FromMilliseconds(100));

            Watch(aut);

            aut.Tell(PoisonPill.Instance, ActorRefs.NoSender);

            ExpectTerminated(aut);

            Unwatch(aut);

            probe.ExpectMsg<DeadLetter>(dl =>
                (string)dl.Message == "Hi, stash this message.");
            probe.ExpectMsg<DeadLetter>(dl =>
                (string)dl.Message == "Stash this as well.");

            Sys.EventStream.Unsubscribe(probe.Ref);
        }

        [Fact]
        public void UnprocessedMailboxBecomesDeadLetters()
        {
            var probe = CreateTestProbe();
            Sys.EventStream.Subscribe(probe.Ref, typeof(DeadLetter));

            var frozenActor = Sys.ActorOf<FrozenActor>();

            Watch(frozenActor);

            frozenActor.Tell("Freeze!");
            frozenActor.Tell("First message that stays in the mailbox.");
            frozenActor.Tell("Second message that stays in the mailbox.");
            frozenActor.Tell("Third message that stays in the mailbox.");

            var cts = ExpectMsg<CancellationTokenSource>();
            cts.Cancel(); // Now, the frozen actor should stop itself.

            ExpectTerminated(frozenActor); // Assert termination.

            Unwatch(frozenActor);

            probe.ExpectMsg<DeadLetter>(dl =>
                (string)dl.Message == "First message that stays in the mailbox.");
            probe.ExpectMsg<DeadLetter>(dl =>
                (string)dl.Message == "Second message that stays in the mailbox.");
            probe.ExpectMsg<DeadLetter>(dl =>
                (string)dl.Message == "Third message that stays in the mailbox.");

            Sys.EventStream.Unsubscribe(probe.Ref);
        }

        [Fact]
        public void ForwardingActorForwardsScheduledMessagesToTestActor()
        {
            var forwardingActor = Sys.ActorOf(Props.Create<ForwardingActor>(TestActor));

            Sys.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(500), forwardingActor, "Cyclic message.", ActorRefs.NoSender);

            ExpectNoMsg(TimeSpan.FromMilliseconds(90));

            ExpectMsg("Cyclic message.", TimeSpan.FromMilliseconds(520));

            ExpectMsg("Cyclic message.", TimeSpan.FromMilliseconds(520));

            ExpectNoMsg(TimeSpan.FromMilliseconds(490));
        }

        [Fact]
        public async Task EscalatesOnlyNullToSupervisor()
        {
            var supervisor = Sys.ActorOf<SupervisorActor>();
            var probe = CreateTestProbe();

            supervisor.Tell(probe.Ref);
            var child = await supervisor.Ask<IActorRef>(Props.Create<SupervisedActor>());

            child.Tell("get");
            ExpectMsg(string.Empty);

            child.Tell("New state.");
            ExpectNoMsg(TimeSpan.FromMilliseconds(100));

            child.Tell(new InvalidOperationException());
            ExpectNoMsg(TimeSpan.FromMilliseconds(200));

            child.Tell("get");
            ExpectMsg("New state.");

            Watch(supervisor);
            Sys.EventStream.Subscribe(probe.Ref, typeof(DeadLetter));

            child.Tell(new NullReferenceException());
            probe.ExpectMsg<string>((msg, sender) =>
                msg == "I'm gonna commit seppuku." && sender.Equals(supervisor));
            ExpectTerminated(supervisor);

            Unwatch(supervisor);

            child.Tell("Are you alive?");
            probe.ExpectMsg<DeadLetter>(dl =>
                (string)dl.Message == "Are you alive?"); // Child is killed along with supervisor
                                                         // therefore the last msg becomes a dead letter.

            Sys.EventStream.Unsubscribe(probe.Ref);
        }

        [Fact]
        public async Task ActorRefsAreEqualToThoseOfActorSelection()
        {
            var actor0 = Sys.ActorOf(Props.Empty, "actor0");
            var actor1 = Sys.ActorOf(Props.Empty, "actor1");

            var actor0_ = await ActorSelection(actor0.Path.ToString()).ResolveOne(TimeSpan.FromMilliseconds(100));
            var actor1_ = await ActorSelection(actor1.Path.ToString()).ResolveOne(TimeSpan.FromMilliseconds(100));

            Assert.True(actor0.Equals(actor0_));
            Assert.True(actor1.Equals(actor1_));
        }

        [Fact]
        public async Task CoordinatedShutdownTerminatesActorSystemGracefully()
        {
            var system0 = ActorSystem.Create("system0");
            var actorOfSystem0 = system0.ActorOf(Props.Empty);
            var system1 = ActorSystem.Create("system1");
            var actorOfSystem1 = system1.ActorOf(Props.Empty);

            Watch(actorOfSystem0);
            Watch(actorOfSystem1);

            ExpectNoMsg(TimeSpan.FromMilliseconds(250));

            var shutdownTask =
                CoordinatedShutdown.Get(system0)
                    .Run(CoordinatedShutdown.ClrExitReason.Instance);

            await shutdownTask;

            ExpectTerminated(actorOfSystem0);
            ExpectNoMsg(250);

            Unwatch(actorOfSystem0);
            Unwatch(actorOfSystem1);
        }

        public sealed class SimulatingHardWorkActor : ReceiveActor
        {
            private readonly IActorRef _countRecipient;
            private int _counter;
            public SimulatingHardWorkActor(IActorRef countRecipient)
            {
                _countRecipient = countRecipient;

                Receive<string>(str => str == "get", str =>
                    _countRecipient.Tell(_counter));

                Receive<object>(o =>
                {
                    Thread.Sleep(10);
                    _counter++;
                });
            }
        }

        [Fact]
        public void RouterResizesDueToTrafficSimulatedByBroadcast()
        {
            var probe = CreateTestProbe();

            var resizer = new DefaultResizer(
                1,
                10,
                1,
                5D,
                0.01D,
                0D,
                1);

            var randomPoolRouter =
                new RandomPool(1).WithResizer(resizer);
            var randomPoolRouterProps = Props.Create<SimulatingHardWorkActor>(probe.Ref).WithRouter(randomPoolRouter);
            var randomPoolRouterActor = Sys.ActorOf(randomPoolRouterProps, "randPoolRouter");

            var broadcastRouterActor =
                Sys.ActorOf(Props.Create<ForwardingActor>(randomPoolRouterActor).WithRouter(new BroadcastPool(50)), "bcastRouter");

            randomPoolRouterActor.Tell(GetRoutees.Instance);
            var routeesBefore = ExpectMsg<Routees>();
            Assert.Equal(routeesBefore.Members.Count(), randomPoolRouter.NrOfInstances);

            broadcastRouterActor.Tell("SPAM");
            Task.Delay(1000).Wait(); // After this delay, first message batch should be processed (probably by only one routee).
            broadcastRouterActor.Tell("SPAM");
            Task.Delay(1000).Wait(); // After this delay, second message batch should be processed by more than one routee.

            randomPoolRouterActor.Tell(new Broadcast("get"));
            var msgList = probe.ReceiveWhile(TimeSpan.FromMilliseconds(1000), o => o);
            Assert.Equal(100, msgList.Aggregate(0, (agg, curr) => (int)curr + agg));

            randomPoolRouterActor.Tell(GetRoutees.Instance);
            var routeesAfter = ExpectMsg<Routees>();
            Assert.Equal(10, routeesAfter.Members.Count()); // Should be already resized to the upper pool limit.
        }

        #region FSM Events

        public sealed class ConfigureEvent
        {
            public string FilePath { get; }

            public ConfigureEvent(string filePath)
            {
                FilePath = filePath;
            }
        }

        public sealed class AppendEvent
        {
            public string StrToAppend { get; }

            public AppendEvent(string strToAppend)
            {
                StrToAppend = strToAppend;
            }
        }

        public sealed class FlushEvent
        {
            private FlushEvent() { }

            static FlushEvent()
            {
                Instance = new FlushEvent();
            }
            public static FlushEvent Instance { get; }
        }

        #endregion

        #region FSM States

        public enum State
        {
            Buffering,
            Closed
        }

        #endregion

        #region FSM Data

        public interface IBurstWriterData { } // Marker interface.

        public sealed class Uninitialized : IBurstWriterData
        {
            private Uninitialized() { }
            // Tell the c# compiler not to mark the type as beforefieldinit.
            static Uninitialized()
            {
                Instance = new Uninitialized();
            }
            public static Uninitialized Instance { get; }
        }

        public sealed class Buffer : IBurstWriterData
        {
            public string Path { get; }
            public IEnumerable<string> Queue { get; }

            public Buffer(string path, IEnumerable<string> queue)
            {
                Path = path;
                Queue = queue;
            }
        }

        #endregion

        public sealed class BurstWriterActor : FSM<State, IBurstWriterData>
        {
            private readonly ILoggingAdapter _logger = Context.GetLogger();
            private readonly TimeSpan _bufferingTimeout;
            public BurstWriterActor(TimeSpan bufferingTimeout)
            {
                _bufferingTimeout = bufferingTimeout;

                StartWith(State.Closed, Uninitialized.Instance);

                When(State.Closed, state =>
                {
                    return state.FsmEvent switch
                    {
                        ConfigureEvent ce when state.StateData is Uninitialized => GoTo(State.Buffering)
                            .Using(new Buffer(ce.FilePath, Enumerable.Empty<string>())),
                        AppendEvent ae when state.StateData is Buffer b => GoTo(State.Buffering)
                            .Using(new Buffer(b.Path, b.Queue.Append(ae.StrToAppend))),
                        _ => null
                    };
                });

                When(State.Buffering, state =>
                {
                    return state.FsmEvent switch
                    {
                        AppendEvent ae when state.StateData is Buffer b => GoTo(State.Buffering)
                            .Using(new Buffer(b.Path, b.Queue.Append(ae.StrToAppend))),
                        StateTimeout _ => GoTo(State.Closed),
                        _ => null
                    };
                }, _bufferingTimeout);

                WhenUnhandled(state =>
                {
                    if (state.FsmEvent is FlushEvent && state.StateData is Buffer b)
                    {
                        using var fs = new FileStream(b.Path,
                            FileMode.OpenOrCreate,
                            FileAccess.Write,
                            FileShare.Read);
                        fs.Position = fs.Length;
                        using var tw = new StreamWriter(fs);

                        tw.Write(string.Concat(b.Queue));

                        return GoTo(State.Closed).Using(new Buffer(b.Path, Enumerable.Empty<string>()));
                    }
                    else
                    {
                        _logger.Warning("Received unhandled request {0} in state {1}/{2}", state.FsmEvent, StateName, state.StateData);
                        return Stay();
                    }
                });

                OnTransition((prevState, nextState) =>
                {
                    if (prevState == State.Buffering && nextState == State.Closed)
                        if (StateData is Buffer b)
                            Self.Tell(FlushEvent.Instance);
                });
            }
        }

        [Fact]
        public async Task FsmActorImplementsBurstWriterWithProtectedResource()
        {
            var texts = "Start string.;Next string.\n;End string.".Split(';', StringSplitOptions.RemoveEmptyEntries);

            var aut = Sys.ActorOf(Props.Create<BurstWriterActor>(TimeSpan.FromSeconds(1)));

            var tempFilePath = Path.GetTempFileName();
            aut.Tell(new ConfigureEvent(tempFilePath));

            aut.Tell(new AppendEvent(texts[0]));
            Thread.Sleep(1500);
            aut.Tell(new AppendEvent(texts[1]));
            Thread.Sleep(500);
            aut.Tell(new AppendEvent(texts[2]));

            var expectedFileContent = string.Concat(texts);
            var actualFileContent =
                await WaitForFileContentAsync(expectedFileContent, tempFilePath, TimeSpan.FromSeconds(3.5));

            Assert.Equal(expectedFileContent, actualFileContent);
        }

        public static Task<string> WaitForFileContentAsync(string expectedSubcontent, string filePath, TimeSpan? timeout = null)
        {
            return Task.Run(() =>
            {
                string content;
                var stopwatch = Stopwatch.StartNew();
                try
                {
                    do
                    {
                        using var fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.Read,
                            FileShare.ReadWrite);
                        using var sr = new StreamReader(fs);
                        content = sr.ReadToEnd();

                        Thread.Sleep(TimeSpan.FromMilliseconds(100));

                        if (stopwatch.Elapsed > (timeout ?? TimeSpan.MaxValue))
                            throw new TimeoutException();
                    } while (!content.Contains(expectedSubcontent));
                }
                catch (FileNotFoundException)
                {
                    content = string.Empty;
                }
                return content;
            });
        }

        public class ParentActor : UntypedActor
        {
            private static object? _privateField;
            public class ChildActor : UntypedActor
            {
                protected override void OnReceive(object message)
                {
                    _privateField = message;
                    Sender.Tell("ok");
                }
            }

            protected override void OnReceive(object message)
            {
                if (message is "get")
                    Sender.Tell(_privateField);
            }
        }

        [Fact]
        public async Task NestedChildActorDirectlyUpdatesParentPrivateStaticField()
        {
            var parentActor = Sys.ActorOf<ParentActor>();
            var nestedChildActor = Sys.ActorOf<ParentActor.ChildActor>();

            nestedChildActor.Tell("updated field");
            ExpectMsg("ok");
            var answer = await parentActor.Ask<string>("get");

            Assert.Equal("updated field", answer);
        }

        public class GrimReaperActor : ReceiveActor
        {
            private class CheckIfAllDead { }
            private class BroadcastIdentification { }

            private IImmutableSet<IActorRef> _markedWithDeath = ImmutableHashSet<IActorRef>.Empty;
            private IImmutableSet<IActorRef> _collected = ImmutableHashSet<IActorRef>.Empty;

            public GrimReaperActor(string actorPath, TimeSpan markWithDeathInterval, TimeSpan collectSoulsInterval)
            {
                var actorSelection = Context.System.ActorSelection(actorPath);

                Context.System.Scheduler.ScheduleTellRepeatedly(
                    TimeSpan.Zero,
                    markWithDeathInterval,
                    Self,
                    new BroadcastIdentification(),
                    Self
                    );

                Context.System.Scheduler.ScheduleTellRepeatedly(
                    TimeSpan.Zero,
                    collectSoulsInterval,
                    Self,
                    new CheckIfAllDead(),
                    Self
                    );

                Receive<ActorIdentity>(ai =>
                {
                    _markedWithDeath = _markedWithDeath.Add(ai.Subject);
                    Context.Watch(ai.Subject);
                });

                Receive<Terminated>(t =>
                {
                    _collected = _collected.Add(t.ActorRef);
                    Context.Unwatch(t.ActorRef);
                });

                Receive<CheckIfAllDead>(_ =>
                {
                var setDiff = _markedWithDeath.Except(_collected);
                    if (_collected.Count() != 0 && setDiff.Count() == 1 && setDiff.Contains(Self))
                    {
                        Context.Stop(Self);
                        Context.System.Terminate();
                    }
                });

                Receive<BroadcastIdentification>(_ => actorSelection.Tell(new Identify(new object())));

                // Concurrency in actor is considered as bad practice. Spawn a worker actor instead.
                Receive<RequestMarkedWithDeathList>(msg =>
                    Task.Run(() => SpinWait.SpinUntil(() => _markedWithDeath.Count() >= msg.MinCount, msg.Timeout))
                        .PipeTo(Sender, success: success => success ? (object)_markedWithDeath.AsEnumerable() : new TimeoutException()));

                Receive<RequestCollectedList>(msg =>
                    Task.Run(() => SpinWait.SpinUntil(() => _collected.Count() >= msg.MinCount, msg.Timeout))
                        .PipeTo(Sender, success: success => success ? (object)_collected.AsEnumerable() : new TimeoutException()));
            }
        }

        public class RequestMarkedWithDeathList
        {
            public RequestMarkedWithDeathList(int minCount, TimeSpan timeout)
            {
                MinCount = minCount;
                Timeout = timeout;
            }

            public int MinCount { get; }
            public TimeSpan Timeout { get; }
        }
        public class RequestCollectedList
        {
            public RequestCollectedList(int minCount, TimeSpan timeout)
            {
                MinCount = minCount;
                Timeout = timeout;
            }

            public int MinCount { get; }
            public TimeSpan Timeout { get; }
        }

        [Fact]
        public void DeathWatchActorFindsAllUserActorsAndTerminatesSystemWhenAllTheseActorsStop()
        {
            Within(TimeSpan.FromSeconds(3), () =>
            {
                var sys = ActorSystem.Create("mySys");

                var dwaProps = Props.Create<GrimReaperActor>("/user/*", TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(200));
                var dwa = sys.ActorOf(dwaProps, "grimReaper");
                dwa.Tell(new RequestCollectedList(2, TimeSpan.FromSeconds(3)));
                dwa.Tell(new RequestMarkedWithDeathList(4, TimeSpan.FromSeconds(3)));

                var w0 = sys.ActorOf(Props.Empty, "worker0");
                var w1 = sys.ActorOf(Props.Empty, "worker1");
                var w2 = sys.ActorOf(Props.Empty, "worker2");

                var marked = ExpectMsg<IEnumerable<IActorRef>>();
                marked.Should().HaveCount(4);
                marked.Should().Contain(w0);
                marked.Should().Contain(w1);
                marked.Should().Contain(w2);
                marked.Should().Contain(dwa);

                w0.Tell(PoisonPill.Instance);
                w1.Tell(PoisonPill.Instance);

                var collected = ExpectMsg<IEnumerable<IActorRef>>();
                collected.Should().HaveCount(2);
                collected.Should().Contain(w0);
                collected.Should().Contain(w1);

                w2.Tell(PoisonPill.Instance);

                sys.WhenTerminated.Wait(TimeSpan.FromSeconds(3)).Should().BeTrue();
            }, TimeSpan.FromSeconds(1));
        }

        public class DisposableActor : UntypedActor, IDisposable
        {
            private IActorRef _probe;

            public DisposableActor(IActorRef probe)
            {
                _probe = probe;
            }

            public void Dispose()
            {
                _probe.Tell("Dispose called.");
            }

            protected override void OnReceive(object message) { }
        }

        [Fact]
        public void DisposableActorIsDisposedWhenActorSystemStopsHim()
        {
            var da = Sys.ActorOf(Props.Create<DisposableActor>(() => new DisposableActor(TestActor)));
            da.Tell(PoisonPill.Instance);
            ExpectMsg("Dispose called.");
        }
    }
}
