// MultiNodeTestRunner.exe command moved to the Akka.MultiNodeTestRunner project post-build event that is triggered by RUNMNTR constant.

using System;
using System.Linq;
using Akka.Actor;
using Akka.Cluster;
using Akka.Cluster.TestKit;
using Akka.Configuration;
using Akka.Event;
using Akka.Remote.TestKit;
using FluentAssertions;
using Xunit;
using Akka.Cluster.Tools.PublishSubscribe;
using System.Threading;

namespace Akka.NetTests
{
    public class DistributedPublishSubscribeTest : MultiNodeClusterSpec
    {
        public class PubSubSpecConfig : MultiNodeConfig
        {
            public RoleName Node1 { get; }

            public RoleName Node2 { get; }

            public PubSubSpecConfig()
            {
                Node1 = Role("node1");
                Node2 = Role("node2");

                CommonConfig = DebugConfig(true)
                .WithFallback(ConfigurationFactory.ParseString(@"
    akka.remote.retry-gate-closed-for = 5s
    akka.remote.log-remote-lifecycle-events = INFO
    akka.remote.dot-netty.connection-timeout = 25s
    akka.cluster.auto-down-unreachable-after = 10s
    akka.cluster.retry-unsuccessful-join-after = 3s
    akka.cluster.seed-node-timeout = 2s
    akka.cluster.shutdown-after-unsuccessful-join-seed-nodes = off
    akka.coordinated-shutdown.terminate-actor-system = on
    akka.extensions = [""Akka.Cluster.Tools.PublishSubscribe.DistributedPubSubExtensionProvider, Akka.Cluster.Tools""]
    "))
                .WithFallback(MultiNodeClusterSpec.ClusterConfig());
            }
        }

        private class SubscriberActor : ReceiveActor
        {
            public SubscriberActor(IActorRef managerActor)
            {
                var mediator = DistributedPubSub.Get(Context.System).Mediator;
                mediator.Tell(new Subscribe("topic", Self));
                var _logger = Logging.GetLogger(Context);

                // Registered but not necessarily replicated to other nodes - the registry
                // is eventually consistent.
                Receive<SubscribeAck>(sack =>
                {
                    if (sack.Subscribe.Topic == "topic" &&
                        sack.Subscribe.Ref.Equals(Self) &&
                        sack.Subscribe.Group == null)
                        managerActor.Tell("Subscription registered.");
                    _logger.Debug($"#DEBUG# Subscription confirmed.");
                });

                Receive<string>(msg =>
                {
                    managerActor.Tell($"Subed msg: {msg}");
                    _logger.Debug($"#DEBUG# Subscriber actor forwarded msg: {msg} to {managerActor}");
                });
            }
        }

        private class PublisherActor : ReceiveActor
        {
            public PublisherActor()
            {
                var mediator = DistributedPubSub.Get(Context.System).Mediator;
                Receive<string>(msg => mediator.Tell(new Publish("topic", msg)));
            }
        }

        private const string _systemName = "PubSubSystem";
        private const int _actorSystem0Port = 666;
        private const int _actorSystem1Port = 667;
        private readonly TimeSpan _epsilonValueForWithins = TimeSpan.FromSeconds(1);
        private readonly PubSubSpecConfig _config;
        private ActorSystem? _actorSystem;
        private ILoggingAdapter? _logger;

        protected override void AfterTermination()
        {
            RunOn(delegate
            {
                if (_actorSystem != null)
                    Shutdown(_actorSystem);
            }, _config.Node1, _config.Node2);
        }

        protected DistributedPublishSubscribeTest(PubSubSpecConfig config) : base(config, typeof(DistributedPublishSubscribeTest))
        {
            _config = config;
        }

        public DistributedPublishSubscribeTest() : this(new PubSubSpecConfig()) { }

        // Copy Akka.Cluster.Tools to MultiNodeTestRunner out dir.
        // Installing this package via NuGet introduces conflicts to project.
        [MultiNodeFact] // Change to MultiNodeFact when you start implementing.
        public void NodesCommunicateViaDistributedPublishSubscribe()
        {
            Within(TimeSpan.FromSeconds(60), () =>
            {
                // Spawn an actor system on node 1.
                RunOn(() =>
                {
                    // I decided to use my own spawned actors. Not the test ones.
                    _actorSystem = ActorSystem.Create(_systemName,
                        ConfigurationFactory.ParseString("akka.remote.dot-netty.tcp.port = " + _actorSystem0Port)
                                            .WithFallback(Sys.Settings.Config));

                    _logger = Logging.GetLogger(_actorSystem, _actorSystem);

                    var actorSys0Addr = Akka.Cluster.Cluster.Get(_actorSystem).SelfAddress;
                    _logger.Debug($"#DEBUG# Actor System 0 address: {actorSys0Addr}");

                    Assert.Equal($"akka.tcp://{_systemName}@localhost:{_actorSystem0Port}", actorSys0Addr.ToString());
                }, _config.Node1);

                // Spawn an actor system on node 2.
                RunOn(() =>
                {
                    _actorSystem = ActorSystem.Create(_systemName,
                        ConfigurationFactory.ParseString("akka.remote.dot-netty.tcp.port = " + _actorSystem1Port)
                                            .WithFallback(Sys.Settings.Config));

                    _logger = Logging.GetLogger(_actorSystem, _actorSystem);

                    var actorSys1Addr = Akka.Cluster.Cluster.Get(_actorSystem).SelfAddress;
                    _logger.Debug($"#DEBUG# Actor System 1 address: {actorSys1Addr}");

                    Assert.Equal($"akka.tcp://{_systemName}@localhost:{_actorSystem1Port}", actorSys1Addr.ToString());
                }, _config.Node2);

                // Form the culster by joining itself.
                RunOn(() =>
                {
                    var actorSys0Addr = new Address("akka.tcp", _systemName, "localhost", _actorSystem0Port);
                    Akka.Cluster.Cluster.Get(_actorSystem).Join(actorSys0Addr);
                    AwaitAssert(() => Akka.Cluster.Cluster.Get(_actorSystem).State.Members.Count.Should().Be(1));
                    AwaitAssert(() => Akka.Cluster.Cluster.Get(_actorSystem).State.Members.All(x => x.Status == MemberStatus.Up).Should().BeTrue());

                    EnterBarrier("cluster-created");
                }, _config.Node1);

                // Join cluster by joining node 1 and itself.
                RunOn(() =>
                {
                    EnterBarrier("cluster-created");

                    var actorSys0Addr = new Address("akka.tcp", _systemName, "localhost", _actorSystem0Port);
                    var actorSys1Addr = new Address("akka.tcp", _systemName, "localhost", _actorSystem1Port);

                    Akka.Cluster.Cluster.Get(_actorSystem).JoinSeedNodes(new[] { actorSys1Addr, actorSys0Addr });
                    AwaitAssert(() => Akka.Cluster.Cluster.Get(_actorSystem).State.Members.Count.Should().Be(2));
                    AwaitAssert(() => Akka.Cluster.Cluster.Get(_actorSystem).State.Members.All(x => x.Status == MemberStatus.Up).Should().BeTrue());

                    EnterBarrier("cluster-fully-formed");
                }, _config.Node2);

                // Spawn the subscriber.
                RunOn(() =>
                {
                    EnterBarrier("cluster-fully-formed");

                    var subActor = _actorSystem!.ActorOf(Props.Create<SubscriberActor>(TestActor).WithDeploy(Deploy.Local), "subscriber");
                    ExpectMsg<string>(msg => msg == "Subscription registered.");

                    Thread.Sleep(TimeSpan.FromSeconds(1)); // Wait a moment for the registration to finish (hopefully).

                    EnterBarrier("subscriber-spawned");
                }, _config.Node1);

                // Spawn the publisher. Publish a message.
                RunOn(() =>
                {
                    EnterBarrier("subscriber-spawned");

                    var pubActor = _actorSystem!.ActorOf(Props.Create<PublisherActor>().WithDeploy(Deploy.Local), "publisher");
                    pubActor.Tell("Published message.");
                }, _config.Node2);

                RunOn(() =>
                {
                    // It is recommended to load the extension when the actor system is started by defining it in akka.extensions configuration property.
                    // Otherwise it will be activated when first used and then it takes a while for it to be populated.
                    // akka.extensions = ["Akka.Cluster.Tools.PublishSubscribe.DistributedPubSubExtensionProvider,Akka.Cluster.Tools"]
                    ExpectMsg<string>(msg => msg == "Subed msg: Published message.");
                }, _config.Node1);

            }, _epsilonValueForWithins);
        }
    }
}
