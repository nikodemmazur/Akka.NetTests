using System;
using Akka.Actor;
using Akka.TestKit;
using Xunit;

namespace Akka.NetTests
{
    public class ActorTests : TestKit.Xunit2.TestKit
    {
        public sealed class ExperimentalRabbit : ReceiveActor
        {
            public ExperimentalRabbit()
            {
                Receive<string>(_ =>
                {
                    Sender.Tell("Ctor behavior.", Self);
                    Become(SecondBehavior); // Never call Become from ctor. It has no effects then.
                });
            }

            void SecondBehavior()
            {
                Receive<string>(_ =>
                {
                    Sender.Tell("Other behavior.", Self);
                });
            }
        }

        [Fact]
        public async void ActorInitialBehaviorComesFromCtor()
        {
            Props autProps = Props.Create<ExperimentalRabbit>();
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
        public void TestingSystemWatchesDeath()
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

            ExpectMsg<Terminated>(msg => msg.ActorRef.Equals(aut));

            Unwatch(aut);
        }

        [Fact]
        public void ActorStopsRespondingAfterBeingKilled()
        {

        }

        [Fact]
        public void ActorStopsRespondingAfterBeingPoisoned()
        {

        }

        [Fact]
        public void ActorStopsRespondingAfterThrowingEx()
        {

        }

        [Fact]
        public void StashBecomesDeadLettersWhenActorIsKilled()
        {

        }

        [Fact]
        public void UnprocessedMailboxBecomesDeadLetters()
        {

        }
    }
}
