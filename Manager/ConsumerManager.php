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

use Widicorp\KafkaBundle\Handler\MessageHandlerInterface;

/**
 * Class ConsumerManager.
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
     *
     * @return \RdKafka\Message
     */
    public function consume(bool $autoCommit = true): \RdKafka\Message
    {
        return $this->consumer->consume($this->timeoutConsumingQueue);
    }

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
    public function getMessageHandler(): MessageHandlerInterface
    {
        return $this->messageHandler;
    }
}
