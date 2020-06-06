using Akka.Persistence.TestKit;
using System;
using Xunit;

namespace Akka.NetTests
{
    public class PersistentActorTests : PersistenceTestKit
    {
        [Fact]
        public void ActorRecoversItsStateWithEventsWhenRestarted()
        {
            throw new NotImplementedException();
        }

        [Fact]
        public void ActorRecoversItsStateAtOnceWithSnapshotWhenRestarted()
        {
            throw new NotImplementedException();
        }

        [Fact]
        public void SenderOfReplayedMessageIsDeadLetters()
        {
            throw new NotImplementedException();
        }

        [Fact]
        public void DeferAsyncDoesNotStoreEvents()
        {
            throw new NotImplementedException();
        }

        [Fact]
        public void PoisonPillBreaksPersisting()
        {
            throw new NotImplementedException();
        }

        [Fact]
        public void AtLeastOnceDeliveryActorGetsDeliveryConfirmationAfterBeingRestarted()
        {
            throw new NotImplementedException();
        }

        [Fact]
        public void AtLeastOnceDeliveryActorSetsDeliverySnapshotDuringRecovery()
        {
            throw new NotImplementedException();
        }

        [Fact]
        public void ActorStopsOnPersistFailure()
        {
            throw new NotImplementedException();
        }

        [Fact]
        public void ActorIgnoresTheMessageAndContinuesWithTheNextOneOnPersistRejected()
        {
            throw new NotImplementedException();
        }
    }
}