<?php

/*
 * This file is part of the Widicorp KafkaBundle package.
 *
 * (c) Widicorp <info@widitrade.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Widicorp\KafkaBundle\DependencyInjection;

use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\DependencyInjection\Extension\Extension;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\Loader\YamlFileLoader;
use Symfony\Component\DependencyInjection\Definition;

/**
 * Class WidicorpKafkaExtension.
 */
class WidicorpKafkaExtension extends Extension
{
    /**
     * {@inheritdoc}
     *
     * @throws \Symfony\Component\DependencyInjection\Exception\BadMethodCallException
     */
    public function load(array $configs, ContainerBuilder $container)
    {
        $configuration = new Configuration();
        $config = $this->processConfiguration($configuration, $configs);

        $loader = new YamlFileLoader($container, new FileLocator(__DIR__.'/../Resources/config'));
        $loader->load('services.yml');

        $container->setParameter('widicorp_kafka.services_name_prefix', $config['services_name_prefix']);

        $this->loadProducers($container, $config);
        $this->loadConsumers($container, $config);
        $this->loadCommandLogger($container, $config);
    }

    /**
     * @param ContainerBuilder $container
     * @param array            $config
     *
     * @throws \Symfony\Component\DependencyInjection\Exception\BadMethodCallException
     */
    protected function loadProducers(ContainerBuilder $container, array $config)
    {
        foreach ($config['producers'] as $key => $producerData) {
            // Create the producer with the factory
            $producerDefinition = new Definition(
                'Widicorp\KafkaBundle\Manager\ProducerManager',
                [
                    'RdKafka\Producer',
                    $producerData,
                ]
            );

            // Use a factory to build the producer
            $producerDefinition
                ->setFactory([
                    new Reference('widicorp_kafka.producer_factory'),
                    'get',
                ])
                ->setPublic(true)
                ->setLazy(true);

            $this->setEventDispatcher($config, $producerDefinition);

            $container->setDefinition(
                sprintf('%s.producer.%s', $config['services_name_prefix'], $key),
                $producerDefinition
            );
        }
    }

    /**
     * @param ContainerBuilder $container
     * @param array            $config
     */
    protected function loadConsumers(ContainerBuilder $container, array $config)
    {
        foreach ($config['consumers'] as $key => $consumerData) {
            $consumerDefinition = new Definition(
                'Widicorp\KafkaBundle\Manager\ConsumerManager',
                [
                    'RdKafka\KafkaConsumer',
                    $consumerData,
                    new Reference($consumerData['message_handler']),
                ]
            );

            $consumerDefinition
                ->setFactory([
                    new Reference('widicorp_kafka.consumer_factory'),
                    'get',
                ])
                ->setPublic(true)
                ->setLazy(true);

            $container->setDefinition(
                sprintf('%s.consumer.%s', $config['services_name_prefix'], $key),
                $consumerDefinition
            );
        }
    }

    /**
     * @param ContainerBuilder $container
     * @param array            $config
     */
    private function loadCommandLogger(ContainerBuilder $container, array $config)
    {
        if (!$config['logger']['enabled']) {
            return;
        }

        $logger = new Reference($config['logger']['service']);
        $container->getDefinition('widicorp_kafka.command.consumer_topic')
            ->addMethodCall('setLogger', [$logger]);

        $container->setParameter('widicorp_kafka.logger.level', $config['logger']['level']);
    }

    /**
     * @param array      $config
     * @param Definition $definition
     *
     * @throws \Symfony\Component\DependencyInjection\Exception\InvalidArgumentException
     */
    protected function setEventDispatcher(array $config, Definition $definition)
    {
        if (true === $config['event_dispatcher']) {
            $definition->addMethodCall(
                'setEventDispatcher',
                [
                    new Reference('event_dispatcher'),
                ]
            );
        }
    }
}
