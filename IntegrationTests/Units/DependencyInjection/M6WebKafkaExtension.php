<?php

/*
 * This file is part of the Widicorp KafkaBundle package.
 *
 * (c) Widicorp <info@widitrade.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Widicorp\KafkaBundle\Tests\Units\DependencyInjection;

use Widicorp\KafkaBundle\DependencyInjection\WidicorpKafkaExtension as Base;
use Widicorp\KafkaBundle\Tests\Units\BaseUnitTest;
use Symfony\Component\DependencyInjection\ParameterBag\ParameterBag;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\Loader\YamlFileLoader;
use Widicorp\KafkaBundle\Manager\ProducerManager;
use Widicorp\KafkaBundle\Manager\ConsumerManager;

/**
 * Class WidicorpKafkaExtension.
 */
class M6WebKafkaExtension extends BaseUnitTest
{
    public function testShouldProduceAMessageAndConsumeItAfter()
    {
        $container = $this->getContainerForConfiguration('config');
        $container->compile();

        $this
            ->boolean($container->has('widicorp_kafka.consumer.consumer1'))
                ->isTrue()
            ->object($producer = $container->get('widicorp_kafka.producer.producer1'))
                ->isInstanceOf(ProducerManager::class)
            ->variable($producer->produce('\O/'))
            ->object($consumer = $container->get('widicorp_kafka.consumer.consumer1'))
                ->isInstanceOf(ConsumerManager::class)
            ->variable($message1 = $consumer->consume())
            ->variable($message1->payload)
                ->isEqualTo('\O/')
        ;
    }

    /**
     * @param $fixtureName
     *
     * @return ContainerBuilder
     */
    protected function getContainerForConfiguration(string $fixtureName): ContainerBuilder
    {
        $extension = new Base();
        $parameterBag = new ParameterBag(['kernel.debug' => true]);
        $container = new ContainerBuilder($parameterBag);
        $container->set('event_dispatcher', $this->getEventDispatcherMock());
        $container->registerExtension($extension);

        $loader = new YamlFileLoader($container, new FileLocator(__DIR__.'/../../../IntegrationTests/Fixtures/'));
        $loader->load($fixtureName.'.yml');

        return $container;
    }
}
