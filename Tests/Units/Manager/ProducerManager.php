<?php

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
use Widicorp\KafkaBundle\Tests\Units\BaseUnitTest;

/**
 * Class ProducerManager.
 */
class ProducerManager extends BaseUnitTest
{
    public function testShouldProduceAMessage()
    {
        $this
            ->given(
                $this->newTestedInstance(),
                $this->getReadyBase($topicMock = $this->getTopicMock()),
                $return = $this->testedInstance->produce('message')
            )
            ->then
                ->variable($return)
                    ->isNull()
                ->mock($topicMock)
                    ->call('produce')
                        ->withArguments(RD_KAFKA_PARTITION_UA, 0, 'message')
                            ->once()
            ;
    }

    public function testShouldNotProduceAMessageIfPartitionDoesNotExist()
    {
        $eventDispatcherMock = $this->getEventDispatcherMock();
        $this
            ->given(
                $this->newTestedInstance(),
                $this->getReadyBase(),
                $this->testedInstance->setEventDispatcher($eventDispatcherMock),
                $this->testedInstance->produce('message', null, 58)
            )
            ->mock($eventDispatcherMock)
                ->call('dispatch')
                    ->withAtLeastArguments([KafkaEvent::EVENT_NAME])
                        ->never()
        ;
    }

    public function testShouldProduceAMessageWithAKey()
    {
        $this
            ->given(
                $this->newTestedInstance(),
                $this->getReadyBase($topicMock = $this->getTopicMock()),
                $return = $this->testedInstance->produce('message', '12345')
            )
            ->then
                ->variable($return)
                    ->isNull()
                ->mock($topicMock)
                    ->call('produce')
                        ->withArguments(RD_KAFKA_PARTITION_UA, 0, 'message', '12345')
                            ->once()
        ;
    }

    public function testShouldNotifyAnEventWhenProducing()
    {
        $eventDispatcherMock = $this->getEventDispatcherMock();
        $this
            ->given(
                $this->newTestedInstance(),
                $this->getReadyBase(),
                $this->testedInstance->setEventDispatcher($eventDispatcherMock),
                $this->testedInstance->produce('message'),
                $this->mockCallbackProduce()
            )
            ->then
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->withAtLeastArguments([KafkaEvent::EVENT_NAME])
                            ->once()
            ;
    }

    public function testShouldNotifyAnEventWhenProducingWithError()
    {
        $eventDispatcherMock = $this->getEventDispatcherMock();
        $this
            ->given(
                $this->newTestedInstance(),
                $this->getReadyBase(),
                $this->testedInstance->setEventDispatcher($eventDispatcherMock),
                $this->testedInstance->produce('message'),
                $this->mockCallbackProduce(false)
            )
            ->then
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->withAtLeastArguments([KafkaEvent::EVENT_ERROR_NAME])
                            ->once()
        ;
    }

    public function testShouldNotNotifyAnEventWhenProducingAndEventDispatcherSetToFalse()
    {
        $eventDispatcherMock = $this->getEventDispatcherMock();
        $this
            ->given(
                $this->newTestedInstance(),
                $this->getReadyBase(),
                $this->testedInstance->produce('message'),
                $this->mockCallbackProduce()
            )
            ->then
                ->mock($eventDispatcherMock)
                    ->call('dispatch')
                        ->never()
        ;
    }

    public function testShouldThrowAnExceptionWhenSettingLogOnEmptyEntity()
    {
        $this
            ->if($this->newTestedInstance())
            ->exception(function () {
                $this->testedInstance->setLogLevel(2);
            })
                ->isInstanceOf('Widicorp\KafkaBundle\Exceptions\EntityNotSetException')
                ->hasMessage('Entity not set')
            ;
    }

    public function testShouldThrowAnExceptionWhenAddingBrokersOnEmptyEntity()
    {
        $this
            ->if($this->newTestedInstance())
            ->exception(function () {
                $this->testedInstance->addBrokers('127.0.0.1');
            })
                ->isInstanceOf('Widicorp\KafkaBundle\Exceptions\EntityNotSetException')
                ->hasMessage('Entity not set')
        ;
    }

    public function testShouldThrowAnExceptionWhenAddingTopicsOnEmptyEntity()
    {
        $this
            ->if($this->newTestedInstance())
            ->exception(function () {
                $this->testedInstance->addTopic('127.0.0.1', new \RdKafka\TopicConf());
            })
                ->isInstanceOf('Widicorp\KafkaBundle\Exceptions\EntityNotSetException')
                ->hasMessage('Entity not set')
        ;
    }

    public function testShouldThrowAnExceptionWhenAddingTopicsWithNoBroker()
    {
        $this
            ->given(
                $this->newTestedInstance(),
                $this->testedInstance->setProducer($this->getProducerMock())
            )
            ->exception(function () {
                $this->testedInstance->addTopic('topicName', new \RdKafka\TopicConf());
            })
                ->isInstanceOf('Widicorp\KafkaBundle\Exceptions\NoBrokerSetException')
                ->hasMessage('No broker set')
        ;
    }

    public function testShouldThrowAnExceptionWhenAddingTopicsWithNoLogLevel()
    {
        $this
            ->given(
                $this->newTestedInstance(),
                $this->testedInstance->setProducer($this->getProducerMock()),
                $this->testedInstance->addBrokers('127.0.0.1')
            )
            ->exception(function () {
                $this->testedInstance->addTopic('topicName', new \RdKafka\TopicConf());
            })
                ->isInstanceOf('Widicorp\KafkaBundle\Exceptions\LogLevelNotSetException')
                ->hasMessage('Log level not set')
        ;
    }

    /**
     * @return \mock\RdKafka\Producer
     */
    protected function getProducerMock($topicMock = null)
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\Producer();
        $mock->getMockController()->newTopic = $topicMock ?? $this->getTopicMock();

        return $mock;
    }

    /**
     * @param null $topicMock
     */
    protected function getReadyBase($topicMock = null)
    {
        $this->testedInstance->setProducer($this->getProducerMock($topicMock));
        $this->testedInstance->addBrokers('127.0.0.1');
        $this->testedInstance->setLogLevel(3);
        $this->testedInstance->addTopic('name', new \RdKafka\TopicConf(), ['auto.commit.interval.ms' => '1000']);
    }

    /**
     * @return \mock\RdKafka\Topic
     */
    protected function getTopicMock(): \mock\RdKafka\Topic
    {
        $this->mockGenerator->orphanize('__construct');
        $this->mockGenerator->shuntParentClassCalls();

        $mock = new \mock\RdKafka\Topic();
        $mock->getMockController()->produce = null;
        $mock->getMockController()->getName = 'super topic';

        return $mock;
    }

    /**
     * @param bool $resultForProducing
     */
    protected function mockCallbackProduce($resultForProducing = true)
    {
        $this->mockGenerator->shuntParentClassCalls();
        $kafka = new \mock\RdKafka\Producer();
        if ($resultForProducing) {
            $message = new \RdKafka\Message();
            $message->err = RD_KAFKA_RESP_ERR_NO_ERROR;
            $message->payload = 'message';
            $message->topic_name = 'super topic';
            $this->testedInstance->produceResponse($kafka, $message);
        } else {
            $this->testedInstance->produceError($kafka, RD_KAFKA_RESP_ERR__FAIL, 'failed');
        }
    }
}
