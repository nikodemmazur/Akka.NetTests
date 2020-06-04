using Akka.Persistence.TestKit;
using Xunit;

namespace Akka.NetTests
{
    public class PersistentActorTests : PersistenceTestKit
    {
        [Fact]
        public void ActorRecoversItsStateWithEventsWhenRestarted()
        {

        }

        [Fact]
        public void ActorRecoversItsStateAtOnceWithSnapshotWhenRestarted()
        {

        }

        [Fact]
        public void SenderOfReplayedMessageIsDeadLetters()
        {

        }

        [Fact]
        public void DeferAsyncDoesNotStoreEvents()
        {

        }

        [Fact]
        public void PoisonPillBreaksPersisting()
        {

        }

        [Fact]
        public void AtLeastOnceDeliveryActorGetsDeliveryConfirmationAfterBeingRestarted()
        {

        }

        [Fact]
        public void AtLeastOnceDeliveryActorSetsDeliverySnapshotDuringRecovery()
        {

        }

        [Fact]
        public void ActorStopsOnPersistFailure()
        {

        }

        [Fact]
        public void ActorIgnoresTheMessageAndContinuesWithTheNextOneOnPersistRejected()
        {

        }
    }
}