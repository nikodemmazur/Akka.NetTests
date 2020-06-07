using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Akka.Actor;
using Akka.Cluster;
using Akka.Cluster.TestKit;
using Akka.Configuration;
using Akka.Remote.TestKit;
using Akka.TestKit;
using FluentAssertions;
using Xunit;

// MultiNodeTestRunner.exe command:
// cd C:\Repos\Akka.NetTests\bin\x64\Debug\netcoreapp3.1 &
// "C:\Repos\Akka.NetTests\bin\x64\Debug\Akka.MultiNodeTestRunner.exe" Akka.NetTests.dll -Dmultinode.output-directory=C:\Repos\Akka.NetTests\bin\x64\Debug\netcoreapp3.1\MultiNodeTestRunnerOutput -Dmultinode.clear-output=1

namespace Akka.NetTests
{
    public class RestartNode2Spec : MultiNodeClusterSpec
    {
        private TimeSpan EpsilonValueForWithins => new TimeSpan(0, 0, 1);

        public class RestartNode2SpecConfig : MultiNodeConfig
        {
            public RoleName Seed1 { get; }

            public RoleName Seed2 { get; }

            public RestartNode2SpecConfig()
            {
                Seed1 = Role("seed1");
                Seed2 = Role("seed2");

                CommonConfig = DebugConfig(true)
                    .WithFallback(ConfigurationFactory.ParseString(@"
              akka.cluster.auto-down-unreachable-after = 2s
              akka.cluster.retry-unsuccessful-join-after = 3s
              akka.remote.retry-gate-closed-for = 45s
              akka.remote.log-remote-lifecycle-events = INFO
            "))
                    .WithFallback(MultiNodeClusterSpec.ClusterConfig());
            }
        }

        private class Watcher : ReceiveActor
        {
            public Watcher()
            {
                Receive<Address>(a =>
                {
                    _seedNode1Address = a;
                    Sender.Tell("ok");
                });
            }
        }

        readonly RestartNode2SpecConfig _config;

        private readonly Lazy<ActorSystem> _seed1System;
        private readonly Lazy<ActorSystem> _restartedSeed1System;
        private static Address? _seedNode1Address;
        private ImmutableList<Address?> SeedNodes => ImmutableList.Create(_seedNode1Address, GetAddress(_config.Seed2));

        public RestartNode2Spec() : this(new RestartNode2SpecConfig()) { }

        protected RestartNode2Spec(RestartNode2SpecConfig config) : base(config, typeof(RestartNode2Spec))
        {
            _config = config;
            _seed1System = new Lazy<ActorSystem>(() => ActorSystem.Create(Sys.Name, Sys.Settings.Config));
            _restartedSeed1System = new Lazy<ActorSystem>(
                () => ActorSystem.Create(Sys.Name, ConfigurationFactory
                    .ParseString("akka.remote.netty.tcp.port = " + SeedNodes.First()!.Port)
                    .WithFallback(Sys.Settings.Config)));
        }

        protected override void AfterTermination()
        {
            RunOn(() =>
            {
                Shutdown(_seed1System.Value);
                if (SeedNodes.All(a => a != null))
                    Shutdown(_restartedSeed1System.Value);
                //Shutdown(seed1System.Value.WhenTerminated.IsCompleted ? restartedSeed1System.Value : seed1System.Value);
            }, _config.Seed1);
        }

        [MultiNodeFact]
        public void ClusterSeedNodesMustBeAbleToRestartFirstSeedNodeAndJoinOtherSeedNodes()
        {
            Within(TimeSpan.FromSeconds(60), () =>
            {
                RunOn(() =>
                {
                    // seed1System is a separate ActorSystem, to be able to simulate restart
                    // we must transfer its address to seed2
                    Sys.ActorOf(Props.Create<Watcher>().WithDeploy(Deploy.Local), "address-receiver");
                    EnterBarrier("seed1-address-receiver-ready");
                }, _config.Seed2);


                RunOn(() =>
                {
                    EnterBarrier("seed1-address-receiver-ready");
                    _seedNode1Address = Akka.Cluster.Cluster.Get(_seed1System.Value).SelfAddress;
                    var seedNode1Address = Akka.Cluster.Cluster.Get(_seed1System.Value).SelfAddress;
                    Sys.ActorSelection(new RootActorPath(GetAddress(_config.Seed2)) / "user" / "address-receiver").Tell(seedNode1Address);
                    ExpectMsg("ok", TimeSpan.FromSeconds(5));
                    EnterBarrier("seed1-address-transferred");
                }, _config.Seed1);

                // now we can join seed1System, seed2 together
                RunOn(() =>
                {
                    Akka.Cluster.Cluster.Get(_seed1System.Value).JoinSeedNodes(SeedNodes);
                    AwaitAssert(() => Akka.Cluster.Cluster.Get(_seed1System.Value).State.Members.Count.Should().Be(2));
                    AwaitAssert(() => Akka.Cluster.Cluster.Get(_seed1System.Value).State.Members.All(x => x.Status == MemberStatus.Up).Should().BeTrue());
                }, _config.Seed1);

                RunOn(() =>
                {
                    EnterBarrier("seed1-address-transferred");
                    Cluster.JoinSeedNodes(SeedNodes);
                    AwaitMembersUp(2);
                }, _config.Seed2);
                EnterBarrier("started");

                // shutdown seed1System
                RunOn(() =>
                {
                    Shutdown(_seed1System.Value, RemainingOrDefault);
                }, _config.Seed1);
                EnterBarrier("seed1-shutdown");

                RunOn(() =>
                {
                    Akka.Cluster.Cluster.Get(_restartedSeed1System.Value).JoinSeedNodes(SeedNodes);
                    Within(TimeSpan.FromSeconds(30), () =>
                    {
                        AwaitAssert(() => Akka.Cluster.Cluster.Get(_restartedSeed1System.Value).State.Members.Count.Should().Be(2));
                        AwaitAssert(() => Akka.Cluster.Cluster.Get(_restartedSeed1System.Value).State.Members.All(x => x.Status == MemberStatus.Up).Should().BeTrue());
                    }, EpsilonValueForWithins);
                }, _config.Seed1);

                RunOn(() =>
                {
                    AwaitMembersUp(2);
                }, _config.Seed2);
                EnterBarrier("seed1-restarted");
            }, EpsilonValueForWithins);
        }

        [Fact] // Change to MultiNodeFact when you start implementing.
        public void NodesCommunicateViaDistributedPublishSubscribe()
        {
            throw new NotImplementedException();
        }

        [Fact] // Change to MultiNodeFact when you start implementing.
        public void NodesEstablishTheStream()
        {
            throw new NotImplementedException();
        }
    }
}
