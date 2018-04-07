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

namespace Widicorp\KafkaBundle\Tests\Units;

use atoum\test;

/**
 * Class BaseUnitTest.
 */
class BaseUnitTest extends test
{
    /**
     * @return \mock\Symfony\Component\EventDispatcher\EventDispatcherInterface
     */
    protected function getEventDispatcherMock(): \mock\Symfony\Component\EventDispatcher\EventDispatcherInterface
    {
        $this->mockGenerator->orphanize('__construct');
        $this->getMockGenerator()->shuntParentClassCalls();

        $mock = new \mock\Symfony\Component\EventDispatcher\EventDispatcherInterface();

        return $mock;
    }

    /**
     * @return \mock\RdKafka\Message
     */
    protected function getMessageMock($noError): \mock\RdKafka\Message
    {
        $this->mockGenerator->orphanize('__construct');

        $mock = new \mock\RdKafka\Message();

        $mock->payload = 'message';
        $mock->topic_name = 'topicName';
        $mock->partition = 2;
        $mock->offset = 4;
        $mock->err = is_bool($noError) && $noError ? RD_KAFKA_RESP_ERR_NO_ERROR : $noError;

        return $mock;
    }
}
