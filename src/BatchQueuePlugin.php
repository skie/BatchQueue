<?php
declare(strict_types=1);

namespace BatchQueue;

use BatchQueue\Service\BatchManager;
use BatchQueue\Storage\BatchStorageInterface;
use BatchQueue\Storage\RedisBatchStorage;
use BatchQueue\Storage\SqlBatchStorage;
use Cake\Core\BasePlugin;
use Cake\Core\Configure;
use Cake\Core\ContainerInterface;

/**
 * Plugin for BatchQueue
 */
class BatchQueuePlugin extends BasePlugin
{
    /**
     * Register application container services.
     *
     * @param \Cake\Core\ContainerInterface $container The Container to update.
     * @return void
     * @link https://book.cakephp.org/5/en/development/dependency-injection.html#dependency-injection
     */
    public function services(ContainerInterface $container): void
    {
        $config = Configure::read('BatchQueue', []);

        $container->add(BatchStorageInterface::class, function () use ($config) {
            $storageType = $config['storage'] ?? 'sql';

            if ($storageType === 'redis') {
                $redisConfig = $config['redis'] ?? [];

                return new RedisBatchStorage($redisConfig);
            } else {
                return new SqlBatchStorage();
            }
        });

        $container->add(BatchManager::class)
            ->addArgument(BatchStorageInterface::class);
    }
}
