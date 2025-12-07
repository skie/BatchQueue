<?php
declare(strict_types=1);

namespace BatchQueue\Test\TestCase\Integration;

use BatchQueue\Service\BatchManager;
use BatchQueue\Storage\SqlBatchStorage;
use BatchQueue\Test\Support\TestJobs\SimpleTestJob;
use Cake\Console\TestSuite\ConsoleIntegrationTestTrait;
use Cake\Core\Configure;
use Cake\ORM\TableRegistry;
use Cake\Queue\QueueManager;
use Cake\TestSuite\TestCase;
use Exception;

/**
 * Test BatchQueue using actual queue worker command execution
 */
class CommandWorkerTest extends TestCase
{
    use ConsoleIntegrationTestTrait;

    protected array $fixtures = ['plugin.BatchQueue.Batches', 'plugin.BatchQueue.BatchJobs'];

    protected function setUp(): void
    {
        parent::setUp();
        $this->setAppNamespace();
        $this->clearAllQueues();

        if (!class_exists('TestApp\Application')) {
            require_once dirname(dirname(__DIR__)) . DS . 'TestApp' . DS . 'Application.php';
        }

        $this->configApplication(
            'TestApp\Application',
            [CONFIG],
        );

        $this->registerQueueConfigs();
        $this->clearAllQueues();
        SimpleTestJob::reset();
    }

    protected function registerQueueConfigs(): void
    {
        foreach (Configure::read('Queue') as $key => $data) {
            if (QueueManager::getConfig($key) === null) {
                QueueManager::setConfig($key, $data);
            }
        }
    }

    protected function tearDown(): void
    {
        $this->clearAllQueues();
        parent::tearDown();
    }

    /**
     * Test real batch of 3 jobs using actual queue worker command
     */
    public function testRealBatchOf3BatchJobs(): void
    {
        $storage = new SqlBatchStorage();
        $batchManager = new BatchManager($storage);

        $batch = $batchManager->batch([
            SimpleTestJob::class,
            SimpleTestJob::class,
            SimpleTestJob::class,
        ]);
        $batchId = $batch->dispatch();

        $this->refreshQM();

        $batchJobCount = $this->countMessages('batchjob');
        $this->assertEquals(3, $batchJobCount, 'Should have 3 jobs queued to batchjob queue');

        $batchJobCount = $this->countMessages('batchjob');

        $this->assertEquals(3, $batchJobCount, 'Should have 3 jobs queued to batchjob queue');

        try {
            $this->exec('queue worker --config=batchjob --queue=batchjob --max-jobs=6 --max-runtime=10');
        } catch (Exception $e) {
            echo 'EXCEPTION during worker command: ' . $e->getMessage() . "\n";
            echo 'Stack trace: ' . $e->getTraceAsString() . "\n";
            throw $e;
        }

        TableRegistry::getTableLocator()->get('BatchQueue.Batches');

        $storage = new SqlBatchStorage();
        $batchData = $storage->getBatch($batchId);

        $this->assertEquals('completed', $batchData->status, 'Batch should be completed');
        $this->assertEquals(3, $batchData->completedJobs, 'All 3 jobs should be completed');
    }

    /**
     * Test real chain of 2 jobs using actual queue worker command
     */
    public function testRealChainOf2Jobs(): void
    {
        $storage = new SqlBatchStorage();
        $batchManager = new BatchManager($storage);

        $chain = $batchManager->chain([
            SimpleTestJob::class,
            SimpleTestJob::class,
        ]);
        $batchId = $chain->dispatch();
        $this->refreshQM();

        $chainedCount = $this->countMessages('chainedjobs');
        $this->assertEquals(1, $chainedCount, 'Should have 1 chain job queued initially');

        try {
            $this->exec('queue worker --config=chainedjobs --max-jobs=3 --max-runtime=5 -v');
        } catch (Exception $e) {
            throw $e;
        }
        $batchData = $storage->getBatch($batchId);
        $this->refreshQM();

        $chainedCount = $this->countMessages('chainedjobs');
        $this->assertEquals(1, $chainedCount, 'Second chain job should be queued');

        try {
            $this->exec('queue worker --config=chainedjobs --max-jobs=1 --max-runtime=5 -v');
        } catch (Exception $e) {
            echo 'EXCEPTION during worker command: ' . $e->getMessage() . "\n";
            echo 'Stack trace: ' . $e->getTraceAsString() . "\n";
            throw $e;
        }

        $batchData = $storage->getBatch($batchId);

        $this->assertEquals('completed', $batchData->status, 'Chain should be completed');
        $this->assertEquals(2, $batchData->completedJobs, 'Both jobs should be completed');
    }

    private function countMessages(string $queueName): int
    {
        $queueName = 'enqueue.app.' . $queueName;

        $enqueueTable = TableRegistry::getTableLocator()->get('Cake/Enqueue.Enqueue');

        return $enqueueTable->find()
            ->where(['queue' => $queueName])
            ->count();
    }

    private function listAllQueueFiles(): void
    {
        $queues = ['default', 'batch', 'batchjob', 'chainedjobs'];

        echo "\n=== All Queue Messages ===\n";
        foreach ($queues as $queueName) {
            $count = $this->countMessages($queueName);
            echo "  Queue '{$queueName}': {$count} messages\n";
        }
    }

    private function refreshQM(): void
    {
        QueueManager::drop('default');
        QueueManager::drop('batchjob');
        QueueManager::drop('chainedjobs');
    }

    private function clearAllQueues(): void
    {
        $this->clearQueue('default');
        $this->clearQueue('batchjob');
        $this->clearQueue('chainedjobs');
    }

    private function clearQueue(string $queueName): void
    {
        $enqueueTable = TableRegistry::getTableLocator()->get('Cake/Enqueue.Enqueue');
        $enqueueTable->deleteAll(['queue LIKE' => '%' . $queueName . '%']);
    }
}
