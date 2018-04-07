<?php

/*
 * This file is part of the Widicorp KafkaBundle package.
 *
 * (c) Widicorp <info@widitrade.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Widicorp\KafkaBundle\Factory;

use Widicorp\KafkaBundle\Manager\ProducerManager;

/**
 * Class ProducerFactory.
 */
class ProducerFactory extends AbstractKafkaFactory
{
    /**
     * @param string $producerClass
     * @param array  $producerData
     *
     * @return ProducerManager
     */
    public function get(string $producerClass, array $producerData): ProducerManager
    {
        $producerManager = new ProducerManager();
        $this->getReadyConfiguration($producerData['configuration']);

        $this->configuration->setDrMsgCb([$producerManager, 'produceResponse']);
        $this->configuration->setErrorCb([$producerManager, 'produceError']);

        $producer = new $producerClass($this->configuration);

        $producerManager->setProducer($producer);
        $producerManager
            ->addBrokers(implode(',', $producerData['brokers']))
            ->setLogLevel($producerData['log_level'])
            ->setEventsPollTimeout($producerData['events_poll_timeout'])
        ;

        foreach ($producerData['topics'] as $topicName => $topic) {
            $this->getReadyTopicConf($topic['configuration']);
            $this->topicConfiguration->setPartitioner((int) $topic['strategy_partition']);
            $producerManager->addTopic($topicName, $this->topicConfiguration);
        }

        return $producerManager;
    }
}
