<?php

/*
 * This file is part of the Widicorp KafkaBundle package.
 *
 * (c) Widicorp <info@widitrade.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Widicorp\KafkaBundle\Manager;

use Widicorp\KafkaBundle\Exceptions\EntityNotSetException;
use Widicorp\KafkaBundle\Exceptions\KafkaException;
use Widicorp\KafkaBundle\Exceptions\LogLevelNotSetException;
use Widicorp\KafkaBundle\Exceptions\NoBrokerSetException;
use Widicorp\KafkaBundle\Helper\NotifyEventTrait;
use RdKafka\Topic;

/**
 * Class ProducerManager.
 */
class ProducerManager
{
    use NotifyEventTrait;

    /**
     * max time in second.
     */
    const MAX_TIME_WAITING_QUEUE_EMPTY = 1;

    /**
     * @var \RdKafka\Producer
     */
    protected $producer;

    /**
     * @var \RdKafka\Topic[]
     */
    protected $topics = [];

    /**
     * @var int
     */
    protected $logLevel;

    /**
     * @var array
     */
    protected $brokers;

    /**
     * event poll timeout in ms.
     *
     * @var int
     */
    protected $eventsPollTimeout;

    /**
     * @return string
     */
    public function getOrigin(): string
    {
        return 'producer';
    }

    /**
     * @param \RdKafka\Producer $entity
     *
     * @return ProducerManager
     */
    public function setProducer(\RdKafka\Producer $entity): self
    {
        $this->producer = $entity;

        return $this;
    }

    /**
     * @param int $logLevel
     *
     * @return ProducerManager
     */
    public function setLogLevel(int $logLevel): self
    {
        $this->checkIfProducerSet();

        $this->producer->setLogLevel($logLevel);
        $this->logLevel = $logLevel;

        return $this;
    }

    /**
     * @param string $brokers
     *
     * @return ProducerManager
     */
    public function addBrokers(string $brokers): self
    {
        $this->checkIfProducerSet();

        $this->producer->addBrokers($brokers);
        $this->brokers = $brokers;

        return $this;
    }

    /**
     * @param int $timeout ms
     *
     * @return ProducerManager
     */
    public function setEventsPollTimeout(int $timeout): self
    {
        $this->eventsPollTimeout = $timeout;

        return $this;
    }

    /**
     * @param string             $name
     * @param \RdKafka\TopicConf $topicConfiguration
     */
    public function addTopic(string $name, \RdKafka\TopicConf $topicConfiguration)
    {
        $this->checkIfProducerSet();
        $this->checkIfBrokersSet();
        $this->checkIfLogLevelSet();

        $this->topics[] = $this->producer->newTopic($name, $topicConfiguration);
    }

    /**
     * @param string      $message
     * @param string|null $key
     * @param int         $partition
     *
     * @throws KafkaException
     */
    public function produce(string $message, string $key = null, int $partition = RD_KAFKA_PARTITION_UA)
    {
        try {
            array_walk($this->topics, $this->produceForEachTopic($message, $partition, $key));
            $startPoll = microtime(true);
            while ($this->producer->getOutQLen() > 0 && microtime(true) - $startPoll < self::MAX_TIME_WAITING_QUEUE_EMPTY) {
                $this->producer->poll($this->eventsPollTimeout);
            }
        } catch (\Exception $e) {
            throw new KafkaException($e->getMessage());
        }
    }

    /**
     * @param \RdKafka\Producer $kafka
     * @param \RdKafka\Message  $message
     *
     * @throws ProducerException
     */
    public function produceResponse(\RdKafka\Producer $kafka, \RdKafka\Message $message)
    {
        // @codingStandardsIgnoreStart
        $messageKey = md5($message->payload.$message->topic_name);
        // @codingStandardsIgnoreEnd

        if (RD_KAFKA_RESP_ERR_NO_ERROR == $message->err) {
            $this->notifyEvent($messageKey);

            return;
        }

        $this->notifyResponseErrorEvent($messageKey, $message->err);
    }

    /**
     * @param \RdKafka\Producer $kafka
     * @param int               $errorCode
     * @param string            $reason
     */
    public function produceError(\RdKafka\Producer $kafka, int $errorCode, string $reason)
    {
        $this->notifyErrorEvent($this->getOrigin(), $errorCode, $reason);
    }

    /**
     * @param string      $message
     * @param int         $partition
     * @param string|null $key
     *
     * @return callable
     */
    protected function produceForEachTopic(string $message, int $partition, string $key = null): callable
    {
        return function (Topic $topic) use ($message, $key, $partition) {
            $this->prepareEvent($this->getOrigin(), md5($message.$topic->getName()));
            /*The second argument is the msgflags. It must be 0 as seen in the documentation:
            https://arnaud-lb.github.io/php-rdkafka/phpdoc/rdkafka-producertopic.produce.html*/
            $topic->produce($partition, 0, $message, $key);
        };
    }

    /**
     * @throws EntityNotSetException
     */
    protected function checkIfProducerSet()
    {
        if (is_null($this->producer)) {
            throw new EntityNotSetException();
        }
    }

    /**
     * @throws NoBrokerSetException
     */
    protected function checkIfBrokersSet()
    {
        if (is_null($this->brokers)) {
            throw new NoBrokerSetException();
        }
    }

    /**
     * @throws LogLevelNotSetException
     */
    protected function checkIfLogLevelSet()
    {
        if (is_null($this->logLevel)) {
            throw new LogLevelNotSetException();
        }
    }
}
