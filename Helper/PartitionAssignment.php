<?php

/*
 * This file is part of the Widicorp KafkaBundle package.
 *
 * (c) Widicorp <info@widitrade.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Widicorp\KafkaBundle\Helper;

use Widicorp\KafkaBundle\Exceptions\KafkaException;

/**
 * Class PartitionAssignment.
 */
class PartitionAssignment
{
    /**
     * @return callable
     */
    public static function handlePartitionsAssignment(): callable
    {
        return function (\RdKafka\KafkaConsumer $consumer, $error, array $partitions = null) {
            switch ($error) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    $consumer->assign($partitions);

                    break;

                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    $consumer->assign(null);

                    break;

                default:
                    throw new KafkaException($error);
            }
        };
    }
}
