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

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

/**
 * This is the class that validates and merges configuration from your app/config files.
 */
class Configuration implements ConfigurationInterface
{
    /**
     * {@inheritdoc}
     */
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('widicorp_kafka', 'array');

        $rootNode
            ->children()
                ->scalarNode('services_name_prefix')->defaultValue('widicorp_kafka')->end()
                ->booleanNode('event_dispatcher')->defaultTrue()->end()
                ->arrayNode('logger')
                    ->children()
                        ->booleanNode('enabled')->defaultValue(false)->end()
                        ->scalarNode('service')->defaultValue('logger')->end()
                        ->scalarNode('level')->defaultValue('error')->end()
                    ->end()
                ->end()
                ->arrayNode('consumers')
                    ->useAttributeAsKey('key')
                    ->prototype('array')
                        ->children()
                            ->arrayNode('configuration')
                                ->prototype('scalar')->end()
                                ->defaultValue([])
                                ->normalizeKeys(false)
                            ->end()
                            ->arrayNode('topicConfiguration')
                                ->prototype('scalar')->end()
                                ->defaultValue([])
                                ->normalizeKeys(false)
                            ->end()
                            ->integerNode('timeout_consuming_queue')->defaultValue(1000)->end()
                            ->scalarNode('message_handler')
                                ->isRequired()
                                ->cannotBeEmpty()
                            ->end()
                            ->arrayNode('topics')
                                ->prototype('scalar')->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('producers')
                    ->useAttributeAsKey('key')
                    ->prototype('array')
                        ->children()
                            ->arrayNode('configuration')
                                ->prototype('scalar')->end()
                                ->defaultValue([])
                                ->normalizeKeys(false)
                            ->end()
                            ->arrayNode('brokers')
                                ->prototype('scalar')->end()
                            ->end()
                            ->integerNode('log_level')->defaultValue(LOG_WARNING)->end()
                            ->integerNode('events_poll_timeout')->defaultValue(500)->end()
                            ->arrayNode('topics')
                                ->useAttributeAsKey('key')
                                ->prototype('array')
                                    ->children()
                                        ->arrayNode('configuration')
                                            ->prototype('scalar')->end()
                                            ->defaultValue([])
                                            ->normalizeKeys(false)
                                        ->end()
                                        ->integerNode('strategy_partition')->end()
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end();

        return $treeBuilder;
    }
}
