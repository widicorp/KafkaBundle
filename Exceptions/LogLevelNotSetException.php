<?php

/*
 * This file is part of the Widicorp KafkaBundle package.
 *
 * (c) Widicorp <info@widitrade.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Widicorp\KafkaBundle\Exceptions;

/**
 * Class LogLevelNotSetException.
 */
class LogLevelNotSetException extends \Exception
{
    protected $message = 'Log level not set';
}
