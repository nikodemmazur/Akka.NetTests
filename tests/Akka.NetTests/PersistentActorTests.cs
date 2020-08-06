using Akka.Actor;
using Akka.Configuration;
using Akka.Configuration.Hocon;
using Akka.Persistence;
using Akka.Persistence.Query;
using Akka.Persistence.Sqlite;
using Akka.Persistence.TestKit;
using Akka.Remote.Transport;
using Akka.Streams.Implementation.Fusing;
using Castle.DynamicProxy.Generators;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Akka.NetTests
{
    public class PersistentActorTests : PersistenceTestKit
    {
        public class Cmd
        {
            public Cmd(string data)
            {
                Data = data;
            }
            public string Data { get; }
        }
        public class BypassEnvelope
        {
            public BypassEnvelope(Cmd cmd)
            {
                Cmd = cmd;
            }
            public Cmd Cmd { get; }
        }
        public class HeavyCmd : Cmd
        {
            public HeavyCmd(string data) : base(data) { }
        }
        public class Evt
        {
            public Evt(string data)
            {
                Data = data;
            }
            public string Data { get; }
        }
        public class State
        {
            private readonly IEnumerable<string> _eventContents;
            public State() : this(Enumerable.Empty<string>()) { }
            public State(IEnumerable<string> eventContents)
            {
                _eventContents = eventContents;
            }
            public State Update(Evt evt) => new State(_eventContents.Append(evt.Data));
            public int Size => _eventContents.Count();
            public override string ToString() => string.Join(", ", _eventContents);
        }

        public class PersistentActor : UntypedPersistentActor
        {
            private State _state = new State();
            private IActorRef _replayedMsgSender = ActorRefs.NoSender;
            private int _replayedEventsCount;

            private void UpdateState(Evt evt)
            {
                _state = _state.Update(evt);
            }

            private void UpdateStateHeavily(Evt evt)
            {
                Thread.Sleep(TimeSpan.FromSeconds(1));
                _state = _state.Update(evt);
            }

            public override string PersistenceId => "PersistenceID";

            protected override void OnCommand(object message)
            {
                switch (message)
                {
                    case Cmd cmd when cmd as HeavyCmd == null:
                        Persist(new Evt($"{cmd.Data} - num of events before: {_state.Size}"), UpdateState);
                        break;
                    case "snap":
                        SaveSnapshot(_state);
                        break;
                    case "restart":
                        throw new Exception();
                    case "print":
                        Sender.Tell(_state.ToString());
                        break;
                    case "shutdown":
                        Context.Stop(Self);
                        break;
                    case "delete":
                        DeleteMessages(long.MaxValue);
                        break;
                    case "sender":
                        Sender.Tell(_replayedMsgSender);
                        break;
                    case BypassEnvelope be:
                        DeferAsync(new Evt($"{be.Cmd.Data} - num of events before: {_state.Size}"), UpdateState);
                        break;
                    case HeavyCmd hc:
                        Persist(new Evt($"{hc.Data} - num of events before: {_state.Size}"), UpdateStateHeavily);
                        break;
                    case "count":
                        Sender.Tell(_replayedEventsCount);
                        break;
                    default:
                        break;
                }
            }

            protected override void OnRecover(object message)
            {
                switch (message)
                {
                    case Evt evt:
                        UpdateState(evt);
                        _replayedMsgSender = Sender;
                        _replayedEventsCount++;
                        break;
                    case SnapshotOffer so when so.Snapshot is State state:
                        _state = state;
                        _replayedMsgSender = Sender;
                        break;
                    case RecoveryCompleted _:
                        DeleteMessages(long.MaxValue);
                        break;
                    default:
                        break;
                }
            }

            protected override void PreRestart(Exception reason, object message)
            {
                Sender.Tell("I'm being restarted.");
                base.PreRestart(reason, message);
            }

            protected override void OnPersistRejected(Exception cause, object @event, long sequenceNr)
            {
                UpdateState((@event as Evt)!); // Fallback; invoke handler in case of rejection anyway
                base.OnPersistRejected(cause, @event, sequenceNr);
            }
        }

        [Fact]
        public void ActorRecoversItsStateWithEventsWhenRestarted()
        {
            var config = Configuration.ConfigurationFactory.ParseString(@"
akka
{
  persistence
  {
    journal
    {
      plugin = ""akka.persistence.journal.sqlite""
      auto-start-journals = [""akka.persistence.journal.sqlite""]
      sqlite
      {
        class = ""Akka.Persistence.Sqlite.Journal.SqliteJournal, Akka.Persistence.Sqlite""
        connection-string = ""data source=C:\\SQLite\\Databases\\AkkaNetTestsEventStore.db""
        auto-initialize : on
      }
    }
  }
}
");
            var sys = ActorSystem.Create("my-sys", config.WithFallback(ConfigurationFactory.Default()));
            SqlitePersistence.Get(sys); // Initialize the extension.
            var pa = sys.ActorOf<PersistentActor>();
            pa.Tell(new Cmd("cmd 0"));
            pa.Tell(new Cmd("cmd 1"));
            pa.Tell("restart");
            ExpectMsg("I'm being restarted.");
            pa.Tell("print");
            //ExpectMsg("cmd 0 - num of events before: 0, cmd 1 - num of events before: 1");
            ExpectMsg<string>(msg => msg.Contains("cmd 0") && msg.Contains("cmd 1"));
            pa.Tell("count");
            ExpectMsg<int>(count => count == 2);
        }

        [Fact]
        public void ActorRecoversItsStateWithEventsAfterBeingStopped()
        {
            var pa = Sys.ActorOf<PersistentActor>();
            pa.Tell(new Cmd("cmd 0"));
            Watch(pa);
            pa.Tell("shutdown");
            ExpectTerminated(pa);
            Unwatch(pa);
            pa = Sys.ActorOf<PersistentActor>();
            pa.Tell("print");
            ExpectMsg("cmd 0 - num of events before: 0");
        }

        [Fact]
        public void ActorIgnoresTheMessageAndContinuesWithTheNextOneOnPersistRejected()
        {
            int counter = default;
            // Reject the first event.
            WithJournalWrite(jwb => jwb.RejectIf(pr => counter++ == default), () => 
            {
                var pa = Sys.ActorOf<PersistentActor>();
                pa.Tell(new Cmd("cmd 0"));
                pa.Tell(new Cmd("cmd 1"));
                pa.Tell("restart");
                ExpectMsg("I'm being restarted.");
                pa.Tell("print");
                ExpectMsg("cmd 1 - num of events before: 1");
            }).Wait();
        }

        [Fact]
        public void ActorRecoversItsStateAtOnceWithSnapshotWhenRestarted()
        {
            // Reject all events.
            WithJournalWrite(jwb => jwb.Reject(), () =>
            {
                var pa = Sys.ActorOf<PersistentActor>();
                pa.Tell(new Cmd("cmd 0"));
                pa.Tell(new Cmd("cmd 1"));
                pa.Tell("snap");
                pa.Tell("restart");
                ExpectMsg("I'm being restarted.");
                pa.Tell("print");
                ExpectMsg("cmd 0 - num of events before: 0, cmd 1 - num of events before: 1");
            }).Wait();
        }

        [Fact]
        public void PersistentActorIsNotOfferedReplayedEventsIfPersistedSnapshotCapturedTheLatestState()
        {
            var pa = Sys.ActorOf<PersistentActor>();
            pa.Tell(new Cmd("cmd 0"));
            pa.Tell(new Cmd("cmd 1"));
            pa.Tell("snap");
            pa.Tell("restart");
            ExpectMsg("I'm being restarted.");
            pa.Tell("print");
            ExpectMsg("cmd 0 - num of events before: 0, cmd 1 - num of events before: 1");
            pa.Tell("count");
            ExpectMsg<int>(count => count == 0);
        }

        [Fact]
        public void SenderOfReplayedMessageIsDeadLetters()
        {
            var pa = Sys.ActorOf<PersistentActor>();
            pa.Tell(new Cmd("cmd 0"));
            pa.Tell("restart");
            ExpectMsg("I'm being restarted.");
            pa.Tell("print");
            ExpectMsg("cmd 0 - num of events before: 0");
            pa.Tell("sender");
            ExpectMsg<IActorRef>(ar => ar.Equals(Sys.DeadLetters));
        }

        [Fact]
        public void DeferAsyncDoesNotStoreEvents()
        {
            var pa = Sys.ActorOf<PersistentActor>();
            pa.Tell(new BypassEnvelope(new Cmd("cmd 0")));
            pa.Tell("print");
            ExpectMsg("cmd 0 - num of events before: 0");
            pa.Tell("restart");
            ExpectMsg("I'm being restarted.");
            pa.Tell("print");
            ExpectMsg(string.Empty);
        }

        [Fact]
        public void PoisonPillBreaksPersisting()
        {
            var pa = Sys.ActorOf<PersistentActor>();
            Watch(pa);
            pa.Tell(new HeavyCmd("cmd 0"));
            pa.Tell(new HeavyCmd("cmd 1"));
            pa.Tell(PoisonPill.Instance);
            ExpectTerminated(pa);
            Unwatch(pa);
            pa = Sys.ActorOf<PersistentActor>();
            pa.Tell("print");
            ExpectMsg<string>(msg => msg != "cmd 0 - num of events before: 0, cmd 1 - num of events before: 1");
        }

        [Fact]
        public void ActorStopsOnPersistFailure()
        {
            WithJournalWrite(jwb => jwb.Fail(), () =>
            {
                var pa = Sys.ActorOf<PersistentActor>();
                Watch(pa);
                pa.Tell(new Cmd("cmd 0"));
                ExpectTerminated(pa);
                Unwatch(pa);
            });
        }

        public class AlodEnvelope
        {
            public AlodEnvelope(long deliveryId, Msg msg)
            {
                DeliveryId = deliveryId;
                Msg = msg;
            }

            public long DeliveryId { get; }
            public Msg Msg { get; }
        }

        public class Msg 
        {
            public Msg(string payload)
            {
                Payload = payload;
            }

            public string Payload { get; }
        }

        public class Confirmation
        {
            public Confirmation(long deliveryId)
            {
                DeliveryId = deliveryId;
            }

            public long DeliveryId { get; }
        }

        public class MsgSent
        {
            public MsgSent(Msg msg)
            {
                Msg = msg;
            }

            public Msg Msg { get; }
        }

        public class MsgConfirmed
        {
            public MsgConfirmed(long deliveryId)
            {
                DeliveryId = deliveryId;
            }

            public long DeliveryId { get; }
        }

        public class AlodActor : AtLeastOnceDeliveryReceiveActor
        {
            private readonly IActorRef _destActor;
            private List<long> _confirmedPersistenceIds = new List<long>();
            private long _currDelId;

            private void Handler(MsgSent msgSent)
            {
                Deliver(_destActor!.Path, l => 
                {
                    return new AlodEnvelope(l, msgSent.Msg);
                });
            }
            private void Handler(MsgConfirmed msgConfirmed, IActorRef requester)
            {
                ConfirmDelivery(msgConfirmed.DeliveryId);
                requester.Tell("Message delivered!");
                _confirmedPersistenceIds.Add(msgConfirmed.DeliveryId);
            }

            public AlodActor(PersistenceSettings.AtLeastOnceDeliverySettings settings, IActorRef destActor) : base(settings)
            {
                _destActor = destActor;

                Recover<MsgSent>(msgSent => Handler(msgSent));
                Recover<MsgConfirmed>(msgConfirmed => Handler(msgConfirmed, ActorRefs.Nobody));
                Recover<SnapshotOffer>(so =>
                {
                    var delSnap = so.Snapshot as AtLeastOnceDeliverySnapshot;
                    _currDelId = delSnap!.CurrentDeliveryId;
                    SetDeliverySnapshot(delSnap);
                });

                Command<Msg>(msg => Persist(new MsgSent(msg), Handler));
                Command<Confirmation>(confirmation => 
                    Persist(new MsgConfirmed(confirmation.DeliveryId), msgConfirmed => Handler(msgConfirmed, Sender)));
                Command<string>(str => str == "print", _ =>
                    Sender.Tell(string.Join(", ", _confirmedPersistenceIds.Select(l => $"{l} confirmed").ToArray())));
                Command<string>(str => str == "snap", delegate
                {
                    var delSnap = GetDeliverySnapshot();
                    SaveSnapshot(delSnap);
                });
                Command<string>(str => str == "delete", delegate
                {
                    var delSnap = GetDeliverySnapshot();
                    DeleteMessages(delSnap.CurrentDeliveryId);
                });
                Command<string>(str => str == "last", delegate
                {
                    Sender.Tell(_currDelId);
                });
            }

            public override string PersistenceId => "somePersistenceId";
        }

        [Fact]
        public void AtLeastOnceDeliveryActorGetsDeliveryConfirmationAfterBeingRecreated()
        {
            var atLeastOnceDeliverySettings = new PersistenceSettings.AtLeastOnceDeliverySettings(TimeSpan.FromMilliseconds(200), 2, 5, 10);
            // TestActor is the destination actor.
            var alodActorProps = Props.Create<AlodActor>(atLeastOnceDeliverySettings, TestActor);
            var alodActor = Sys.ActorOf(alodActorProps);
            alodActor.Tell(new Msg("payload"));
            Thread.Sleep(TimeSpan.FromSeconds(1)); // Allow for some retries.
            ExpectMsg<AlodEnvelope>(alode => LastSender.Tell(new Confirmation(alode.DeliveryId)));
            ReceiveWhile<object>(obj => obj as string == "Message delivered!", TimeSpan.FromSeconds(3));
            ReceiveWhile<object>(obj => true, TimeSpan.FromSeconds(3)); // Discard remaining messages.

            Watch(alodActor);
            alodActor.Tell(PoisonPill.Instance);
            ExpectTerminated(alodActor);
            Unwatch(alodActor);

            alodActor = Sys.ActorOf(alodActorProps);
            ExpectNoMsg(TimeSpan.FromSeconds(5));
            alodActor.Tell("print");
            ExpectMsg<string>(str => str.Contains("1 confirmed"));
        }

        [Fact]
        public void AtLeastOnceDeliveryActorSetsDeliverySnapshotDuringRecovery()
        {
            var atLeastOnceDeliverySettings = new PersistenceSettings.AtLeastOnceDeliverySettings(TimeSpan.FromMilliseconds(200), 2, 5, 10);
            // TestActor is the destination actor.
            var alodActorProps = Props.Create<AlodActor>(atLeastOnceDeliverySettings, TestActor);
            var alodActor = Sys.ActorOf(alodActorProps);
            alodActor.Tell(new Msg("payload"));
            Thread.Sleep(TimeSpan.FromSeconds(1)); // Allow for some retries.
            ExpectMsg<AlodEnvelope>(alode => LastSender.Tell(new Confirmation(alode.DeliveryId)));
            ReceiveWhile<object>(obj => obj as string == "Message delivered!", TimeSpan.FromSeconds(3));
            ReceiveWhile<object>(obj => true, TimeSpan.FromSeconds(3)); // Discard remaining messages.

            alodActor.Tell("snap");
            alodActor.Tell("delete");

            Watch(alodActor);
            alodActor.Tell(PoisonPill.Instance);
            ExpectTerminated(alodActor);
            Unwatch(alodActor);

            alodActor = Sys.ActorOf(alodActorProps);
            ExpectNoMsg(TimeSpan.FromSeconds(5));

            alodActor.Tell("last");
            ExpectMsg<long>(l => l == 1L);
        }
    }
}