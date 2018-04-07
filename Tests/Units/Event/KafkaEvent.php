<?php

/*
 * This file is part of the Widicorp KafkaBundle package.
 *
 * (c) Widicorp <info@widitrade.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Widicorp\KafkaBundle\Tests\Units\Event;

use atoum\test;
use Widicorp\KafkaBundle\Event\KafkaEvent as Base;

/**
 * Class EventLog.
 */
class KafkaEvent extends test
{
    public function testShouldGetACorrectEventAfterConstruction()
    {
        $this
            ->if($event = new Base('consumer'))
            ->then
                ->string($event->getOrigin())
                    ->isEqualTo('consumer')
            ;
    }
}
