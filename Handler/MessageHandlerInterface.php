<?php

/*
 * This file is part of the Widicorp KafkaBundle package.
 *
 * (c) Widicorp <info@widitrade.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Widicorp\KafkaBundle\Handler;

use RdKafka\Message;

interface MessageHandlerInterface
{
    /**
     * Process message from kafka.
     *
     * @param Message $message
     *
     * @return mixed
     */
    public function process(Message $message);

    /**
     * @return mixed
     */
    public function endOfPartitionReached();
}
