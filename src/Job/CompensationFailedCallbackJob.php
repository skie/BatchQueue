<?php
declare(strict_types=1);

namespace BatchQueue\Job;

use BatchQueue\Storage\SqlBatchStorage;
use Cake\Queue\Job\JobInterface;
use Cake\Queue\Job\Message;

/**
 * Compensation Failed Callback Job
 *
 * Called when a compensation chain fails.
 * Updates the original batch context with failure status.
 */
class CompensationFailedCallbackJob implements JobInterface
{
    /**
     * Execute compensation failure callback
     *
     * @param \Cake\Queue\Job\Message $message Job message
     * @return string|null Job result
     */
    public function execute(Message $message): ?string
    {
        $args = $message->getArgument();
        $originalBatchId = $args['original_batch_id'] ?? null;
        $error = $args['error'] ?? 'Unknown error';

        if (!$originalBatchId) {
            return null;
        }

        $storage = new SqlBatchStorage();
        $batch = $storage->getBatch($originalBatchId);

        if ($batch) {
            $context = $batch->context ?? [];
            $context['compensation_status'] = 'failed';
            $context['compensation_failed_at'] = date('Y-m-d H:i:s');
            $context['compensation_error'] = $error;

            $storage->updateBatch($originalBatchId, ['context' => $context]);
        }

        return json_encode(['compensation_failed' => true, 'error' => $error]);
    }
}
