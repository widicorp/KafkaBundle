<?php

namespace Widicorp\KafkaBundle\Manager;

use Widicorp\KafkaBundle\Handler\MessageHandlerInterface;

/**
 * Class ConsumerManager
 * @package Widicorp\KafkaBundle
 *
 * A class to consume messages with topics
 */
class ConsumerManager
{
    /**
     * @var \RdKafka\Message
     */
    protected $message;

    /**
     * @var \RdKafka\Consumer
     */
    protected $consumer;

    /**
     * @var int
     */
    protected $timeoutConsumingQueue;

    /**
     * @var MessageHandlerInterface
     */
    protected $messageHandler;

    /**
     * @return string
     */
    public function getOrigin(): string
    {
        return 'consumer';
    }

    /**
     * @param array $topicNames
     *
     * @return void
     */
    public function addTopic(array $topicNames)
    {
        $this->consumer->subscribe($topicNames);
    }

    /**
     * @param \RdKafka\KafkaConsumer $consumer
     */
    public function setConsumer(\RdKafka\KafkaConsumer $consumer)
    {
        $this->consumer = $consumer;
    }

    /**
     * @param int $timeoutConsumingQueue
     */
    public function setTimeoutConsumingQueue(int $timeoutConsumingQueue)
    {
        $this->timeoutConsumingQueue = $timeoutConsumingQueue;
    }

    /**
     * @param bool $autoCommit
     * @return \RdKafka\Message
     */
    public function consume(bool $autoCommit = true): \RdKafka\Message
    {
        return $this->consumer->consume($this->timeoutConsumingQueue);
    }

    /**
     * @return void
     */
    public function commit()
    {
        $this->consumer->commit($this->message);
    }

    /**
     * @param MessageHandlerInterface $messageHandler
     */
    public function setMessageHandler(MessageHandlerInterface $messageHandler)
    {
        $this->messageHandler = $messageHandler;
    }

    /**
     * @return MessageHandlerInterface
     */
    public function getMessageHandler() : MessageHandlerInterface
    {
        return $this->messageHandler;
    }
}
