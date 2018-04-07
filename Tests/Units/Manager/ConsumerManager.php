<?php

declare(strict_types=1);

/*
 * This file is part of the Widicorp KafkaBundle package.
 *
 * (c) Widicorp <info@widitrade.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Widicorp\KafkaBundle\Tests\Units\Manager;

use Widicorp\KafkaBundle\Event\KafkaEvent;
use Widicorp\KafkaBundle\Manager\ConsumerManager as Base;
use Widicorp\KafkaBundle\Tests\Units\BaseUnitTest;

/**
 * Class ConsumerManager.
 */
class ConsumerManager extends BaseUnitTest
{
    public function testShouldConsumeMessages()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $consumer = $this->getReadyBase($consumerMock = $this->getConsumerMock(), true, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume())
            ->then
                ->object($result)
                    ->isInstanceOf('\RdKafka\Message')
                ->integer(count($result))
                    ->isEqualTo(1)
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->withAtLeastArguments(['widicorp.kafka'])
                            ->once()
                ->mock($consumerMock)
                    ->call('commit')
                        ->withArguments($result)
                            ->once()
        ;
    }

    public function testShouldCommitMessageAfterConsumingAndMessage()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $consumer = $this->getReadyBase($consumerMock = $this->getConsumerMock(), true, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume(false))
            ->and($consumer->commit())
            ->then
                ->object($result)
                    ->isInstanceOf('\RdKafka\Message')
                ->integer(count($result))
                    ->isEqualTo(1)
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->withAtLeastArguments([KafkaEvent::EVENT_NAME])
                            ->once()
                ->mock($consumerMock)
                    ->call('commit')
                        ->withArguments($result)
                            ->once()
        ;
    }

    public function testShouldCommitMessageWhenConsumingMessage()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $consumer = $this->getReadyBase($consumerMock = $this->getConsumerMock(), true, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume(false))
            ->then
                ->object($result)
                    ->isInstanceOf('\RdKafka\Message')
                ->integer(count($result))
                    ->isEqualTo(1)
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->withAtLeastArguments([KafkaEvent::EVENT_NAME])
                            ->once()
                ->mock($consumerMock)
                    ->call('commit')
                        ->never()
        ;
    }

    public function testShouldInformThatTheresNoMoreMessage()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $consumer = $this->getReadyBase($consumerMock = $this->getConsumerMock(RD_KAFKA_RESP_ERR__PARTITION_EOF), true, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume())
            ->then
                ->object($result)
                ->integer($result->err)
                    ->isEqualTo(RD_KAFKA_RESP_ERR__PARTITION_EOF)
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->never()
                ->mock($consumerMock)
                    ->call('commit')
                        ->never()
        ;
    }

    public function testShouldInformThatTheresATimeOut()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $consumer = $this->getReadyBase($consumerMock = $this->getConsumerMock(RD_KAFKA_RESP_ERR__TIMED_OUT), true, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume())
            ->then
                ->object($result)
                ->integer($result->err)
                    ->isEqualTo(RD_KAFKA_RESP_ERR__TIMED_OUT)
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->withAtLeastArguments([KafkaEvent::EVENT_ERROR_NAME])
                            ->once()
                ->mock($consumerMock)
                    ->call('commit')
                        ->never()
        ;
    }

    public function testShouldNotCommitMessageNeitherSendEventIfError()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $consumer = $this->getReadyBase($consumerMock = $this->getConsumerMock(RD_KAFKA_RESP_ERR__BAD_MSG), true, $eventDispatcherMock)
            )
            ->if($consumer->consume())
            ->then
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->withAtLeastArguments([KafkaEvent::EVENT_ERROR_NAME])
                            ->once()
                ->mock($consumerMock)
                    ->call('commit')
                        ->never()
        ;
    }

    public function testShouldConsumeMessagesWithoutSendingEvent()
    {
        $this
            ->given(
                $eventDispatcherMock = $this->getEventDispatcherMock(),
                $consumer = $this->getReadyBase($consumerMock = $this->getConsumerMock(), false, $eventDispatcherMock)
            )
            ->if($result = $consumer->consume())
            ->then
                ->object($result)
                    ->isInstanceOf('\RdKafka\Message')
                ->integer(count($result))
                    ->isEqualTo(1)
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                            ->never()
                ->mock($consumerMock)
                    ->call('commit')
                        ->once()
        ;
    }

    /**
     * @param \RdKafka\KafkaConsumer $consumer
     * @param bool                   $eventDispatcherSet
     * @param null                   $eventDispatcherMock
     *
     * @return Base
     */
    protected function getReadyBase(\RdKafka\KafkaConsumer $consumer, bool $eventDispatcherSet = false, $eventDispatcherMock = null): Base
    {
        $consumerManager = new Base();
        $consumerManager->setConsumer($consumer);
        $consumerManager->addTopic(['name']);
        $consumerManager->setTimeoutConsumingQueue(1000);

        if ($eventDispatcherSet) {
            $consumerManager->setEventDispatcher($eventDispatcherMock);
        }

        return $consumerManager;
    }

    /**
     * @param string $topicName
     *
     * @return \mock\RdKafka\KafkaConsumer
     */
    protected function getConsumerMock($noError = true): \mock\RdKafka\KafkaConsumer
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\KafkaConsumer();
        $mock->getMockController()->consume = $this->getMessageMock($noError);

        return $mock;
    }
}
