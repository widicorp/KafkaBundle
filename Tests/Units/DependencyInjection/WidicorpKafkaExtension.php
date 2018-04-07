<?php

namespace Widicorp\KafkaBundle\Tests\Units\DependencyInjection;

use Widicorp\KafkaBundle\DependencyInjection\WidicorpKafkaExtension as TestedClass;
use Widicorp\KafkaBundle\Manager\ConsumerManager;
use Widicorp\KafkaBundle\Manager\ProducerManager;
use Widicorp\KafkaBundle\Tests\Units\BaseUnitTest;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\Loader\YamlFileLoader;

/**
 * Class WidicorpKafkaExtension
 * @package Widicorp\KafkaBundle\Tests\Units\DependencyInjection
 *
 * A class to test configuration loading
 */
class WidicorpKafkaExtension extends BaseUnitTest
{
    /**
     * @return void
     */
    public function testShouldGetACorrectConfigurationForConsumer()
    {
        $container = $this->getContainerForConfiguration('config');
        $container->compile();

        $this
            ->boolean($container->has('widicorp_kafka.consumer.consumer1'))
                ->isTrue()
            ->object($container->get('widicorp_kafka.consumer.consumer1'))
                ->isInstanceOf('Widicorp\KafkaBundle\Manager\ConsumerManager')
        ;
    }

    /**
     * @return void
     */
    public function testShouldGetACorrectConfigurationForProducer()
    {
        $container = $this->getContainerForConfiguration('config');
        $container->compile();

        $this
            ->boolean($container->has('widicorp_kafka.producer.producer1'))
                ->isTrue()
            ->object($container->get('widicorp_kafka.producer.producer1'))
                ->isInstanceOf('Widicorp\KafkaBundle\Manager\ProducerManager')
        ;
    }

    /**
     * @param string                                                 $fixtureName
     * @param \mock\Symfony\Component\DependencyInjection\Definition $definition
     *
     * @return \mock\Symfony\Component\DependencyInjection\ContainerBuilder
     */
    protected function getContainerForConfiguration(string $fixtureName): \mock\Symfony\Component\DependencyInjection\ContainerBuilder
    {
        $extension = new TestedClass();

        $container = new \mock\Symfony\Component\DependencyInjection\ContainerBuilder();
        $container->set('event_dispatcher', $this->getEventDispatcherMock());
        $container->set('widicorp_kafka.producer_factory', $this->getProducerFactoryMock());
        $container->set('widicorp_kafka.consumer_factory', $this->getConsumerFactoryMock());
        $container->registerExtension($extension);

        $loader = new YamlFileLoader($container, new FileLocator(__DIR__.'/../../../Tests/Fixtures/'));
        $loader->load($fixtureName.'.yml');

        return $container;
    }

    /**
     * @return \mock\Widicorp\KafkaBundle\Factory\ProducerFactory
     */
    protected function getProducerFactoryMock()
    {
        $this->mockGenerator->orphanize('__construct');

        $mock = new \mock\Widicorp\KafkaBundle\Factory\ProducerFactory();
        $mock->getMockController()->get = new ProducerManager();

        return $mock;
    }

    /**
     * @return \mock\Widicorp\KafkaBundle\Factory\ConsumerFactory
     */
    protected function getConsumerFactoryMock()
    {
        $this->mockGenerator->orphanize('__construct');

        $mock = new \mock\Widicorp\KafkaBundle\Factory\ConsumerFactory();
        $mock->getMockController()->get = new ConsumerManager();

        return $mock;
    }
}
