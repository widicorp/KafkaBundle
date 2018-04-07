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

/**
 * Class AbstractKafkaFactory.
 */
class AbstractKafkaFactory
{
    /**
     * @var \RdKafka\Conf
     */
    protected $configuration;

    /**
     * @var \RdKafka\TopicConf
     */
    protected $topicConfiguration;

    /**
     * AbstractKafkaFactory constructor.
     *
     * @param \RdKafka\Conf      $configuration
     * @param \RdKafka\TopicConf $topicConfiguration
     */
    public function __construct(\RdKafka\Conf $configuration, \RdKafka\TopicConf $topicConfiguration)
    {
        $this->configuration = $configuration;
        $this->topicConfiguration = $topicConfiguration;
    }

    /**
     * @param array $configurationToSet
     *
     * @return \RdKafka\Conf
     */
    protected function getReadyConfiguration(array $configurationToSet = [])
    {
        foreach ($configurationToSet as $configKey => $configValue) {
            $this->configuration->set($configKey, $configValue);
        }
    }

    /**
     * @param array $configurationToSet
     *
     * @return \RdKafka\TopicConf
     */
    protected function getReadyTopicConf(array $configurationToSet = [])
    {
        foreach ($configurationToSet as $configKey => $configValue) {
            $this->topicConfiguration->set($configKey, $configValue);
        }
    }
}
