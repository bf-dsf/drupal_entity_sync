<?php

namespace Drupal\entity_sync\Plugin\QueueWorker;

// Rename it to prevent confusion with Drupal's entity manager service.
use Drupal\entity_sync\Export\EntityManagerInterface as ExportEntityManagerInterface;

use Drupal\Core\Config\ConfigFactoryInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Logger\LoggerChannelInterface;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\Queue\QueueWorkerBase;

use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Queue worker for exporting a local entity.
 *
 * @QueueWorker(
 *  id = "entity_sync_export_local_entity",
 *  title = @Translation("Export local entity"),
 *  cron = {"time" = 60}
 * )
 */
class ExportLocalEntity extends QueueWorkerBase implements
  ContainerFactoryPluginInterface {

  /**
   * The `throw` error handling mode.
   *
   * @see config/schema/entity_sync.schema.yml::entity_sync.operation.export_entity.queue_error_handling
   */
  const ERROR_MODE_THROW = 'throw';

  /**
   * The `log_and_skip` error handling mode.
   *
   * @see config/schema/entity_sync.schema.yml::entity_sync.operation.export_entity.queue_error_handling
   */
  const ERROR_MODE_LOG_AND_SKIP = 'log_and_skip';

  /**
   * The `log_and_throw` error handling mode.
   *
   * @see config/schema/entity_sync.schema.yml::entity_sync.operation.export_entity.queue_error_handling
   */
  const ERROR_MODE_LOG_AND_THROW = 'log_and_throw';

  /**
   * The `skip` error handling mode.
   *
   * @see config/schema/entity_sync.schema.yml::entity_sync.operation.export_entity.queue_error_handling
   */
  const ERROR_MODE_SKIP = 'skip';

  /**
   * The configuration factory.
   *
   * @var \Drupal\Core\Config\ConfigFactoryInterface
   */
  protected $configFactory;

  /**
   * The entity type manager.
   *
   * @var \Drupal\Core\Entity\EntityTypeManagerInterface
   */
  protected $entityTypeManager;

  /**
   * This module's logger channel service.
   *
   * @var \Drupal\Core\Logger\LoggerChannelInterface
   */
  protected $logger;

  /**
   * The Entity Sync export entity manager service.
   *
   * @var \Drupal\entity_sync\Export\EntityManagerInterface
   */
  protected $manager;

  /**
   * Constructs a new export instance.
   *
   * @param array $configuration
   *   A configuration array containing information about the plugin instance.
   * @param string $plugin_id
   *   The plugin_id for the plugin instance.
   * @param mixed $plugin_definition
   *   The plugin implementation definition.
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager
   *   The entity type manager.
   * @param \Drupal\entity_sync\Export\EntityManagerInterface $manager
   *   The Entity Sync export entity manager service.
   * @param \Drupal\Core\Config\ConfigFactoryInterface $config_factory
   *   The configuration factory.
   * @param \Drupal\Core\Logger\LoggerChannelInterface $logger
   *   This module's logger channel.
   */
  public function __construct(
    array $configuration,
    $plugin_id,
    $plugin_definition,
    EntityTypeManagerInterface $entity_type_manager,
    ExportEntityManagerInterface $manager,
    ConfigFactoryInterface $config_factory,
    LoggerChannelInterface $logger
  ) {
    parent::__construct($configuration, $plugin_id, $plugin_definition);

    $this->entityTypeManager = $entity_type_manager;
    $this->manager = $manager;
    $this->configFactory = $config_factory;
    $this->logger = $logger;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(
    ContainerInterface $container,
    array $configuration,
    $plugin_id,
    $plugin_definition
  ) {
    return new static(
      $configuration,
      $plugin_id,
      $plugin_definition,
      $container->get('entity_type.manager'),
      $container->get('entity_sync.export.entity_manager'),
      $container->get('config.factory'),
      $container->get('logger.channel.entity_sync')
    );
  }

  /**
   * {@inheritdoc}
   *
   * Data should be an associative array with the following elements:
   * - sync_id: The ID of the synchronization that defines the export operation
   *   to run.
   * - entity_type_id: The type ID of the entity being exported.
   * - entity_id: The ID of the entity being exported.
   *
   * @throws \InvalidArgumentException
   *   When invalid or inadequate data are passed to the queue worker.
   * @throws \RuntimeException
   *   When no entity was found for the given data.
   *
   * @see \Drupal\entity_sync\Export\EntityManager::exportLocalEntity()
   *
   * @I Write tests for the export local entity queue worker
   *    type     : task
   *    priority : normal
   *    labels   : export, testing, queue
   */
  public function processItem($data) {
    $this->validateData($data);

    // Load the entity.
    // @I Should we load the uncached entity?
    //    type     : bug
    //    priority : normal
    //    labels   : cache, export, queue
    $entity = $this
      ->entityTypeManager
      ->getStorage($data['entity_type_id'])
      ->load($data['entity_id']);

    // Load the error handling mode from the synchronization with falling back
    // to the default if it is not defined. If for some reason the sync is not
    // found, such as if it is removed before all of its queue items are
    // processed, an error will be thrown when trying to get the error handling
    // mode here - which is the default error handling behavior.
    $sync = $this->configFactory->get('entity_sync.sync.' . $data['sync_id']);
    $handling = $sync->get('operations.export_entity.queue_error_handling');
    if (!$handling) {
      $handling = self::ERROR_MODE_THROW;
    }

    try {
      if (!$entity) {
        throw new \RuntimeException(
          sprintf(
            'No "%s" entity with ID "%s" found to export.',
            $data['entity_type_id'],
            $data['entity_id']
          )
        );
      }
      $this->manager->exportLocalEntity(
        $data['sync_id'],
        $entity
      );
    }
    catch (\Throwable $throwable) {
      $this->handleThrowable($throwable, $data, $handling);
    }
  }

  /**
   * Handles errors with the given error handling mode.
   *
   * See `config/schema/entity_sync.schema.yml` for an explanation of
   * supported error handling modes.
   *
   * @param \Throwable $throwable
   *   The error that was thrown.
   * @param array $data
   *   The data that was passed to
   *   \Drupal\Core\Queue\QueueInterface::createItem() when the item was queued.
   * @param string $handling
   *   The error handling mode.
   *
   * @throw \RuntimeException
   *   When an unknown error handling mode is requested.
   */
  protected function handleThrowable(
    \Throwable $throwable,
    array $data,
    string $handling
  ) {
    switch ($handling) {
      case NULL:
      case self::ERROR_MODE_THROW:
        $this->handleThrowableWithThrow($throwable);
        break;

      case self::ERROR_MODE_LOG_AND_SKIP:
        $this->handleThrowableWithLogAndSkip($throwable, $data);
        break;

      case self::ERROR_MODE_LOG_AND_THROW:
        $this->handleThrowableWithLogAndThrow($throwable, $data);

      case self::ERROR_MODE_SKIP:
        $this->handleThrowableWithSkip();
        break;

      default:
        throw new \RuntimeException(sprintf(
          'Unknown error handling type "%s"',
          $handling
        ));
    }
  }

  /**
   * Handles errors with the `throw` error handling mode.
   *
   * Errors are thrown; their messages are not logged.
   *
   * @param \Throwable $throwable
   *   The error that was thrown.
   *
   * @throws \Throwable
   *   The error that was originally thrown.
   */
  protected function handleThrowableWithThrow(\Throwable $throwable) {
    throw $throwable;
  }

  /**
   * Handles errors with the `log_and_skip` error handling mode.
   *
   * Errors are not thrown; their messages are logged.
   *
   * @param \Throwable $throwable
   *   The error that was thrown.
   * @param array $data
   *   The data that was passed to
   *   \Drupal\Core\Queue\QueueInterface::createItem() when the item was queued.
   */
  protected function handleThrowableWithLogAndSkip(\Throwable $throwable, array $data) {
    $this->logger->error(
      'A "@throwable_type" error was thrown while executing a queued export of the entity with ID "@entity_id" as part of the "@sync_id" synchronization. The error message was: @throwable_message',
      [
        '@throwable_type' => get_class($throwable),
        '@throwable_message' => $throwable->getMessage(),
        '@sync_id' => $data['sync_id'],
        '@entity_id' => $data['entity_id'],
      ]
    );
  }

  /**
   * Handles errors with the `log_and_throw` error handling mode.
   *
   * Errors are thrown; their messages are logged.
   *
   * @param \Throwable $throwable
   *   The error that was thrown.
   * @param array $data
   *   The data that was passed to
   *   \Drupal\Core\Queue\QueueInterface::createItem() when the item was queued.
   *
   * @throws \Throwable
   *   The error that was originally thrown.
   */
  protected function handleThrowableWithLogAndThrow(\Throwable $throwable, array $data) {
    $this->logger->error(
      'A "@throwable_type" error was thrown while executing a queued export of the entity with ID "@entity_id" as part of the "@sync_id" synchronization. The error message was: @throwable_message',
      [
        '@throwable_type' => get_class($throwable),
        '@throwable_message' => $throwable->getMessage(),
        '@sync_id' => $data['sync_id'],
        '@entity_id' => $data['entity_id'],
      ]
    );
    throw $throwable;
  }

  /**
   * Handles errors with the `skip` error handling mode.
   *
   * Errors are not thrown, their messages are not logged.
   */
  protected function handleThrowableWithSkip() {
  }

  /**
   * Validates that the data passed to the queue item are valid.
   *
   * @param mixed $data
   *   The data.
   *
   * @throws \InvalidArgumentException
   *   When the data are invalid.
   */
  protected function validateData($data) {
    if (!is_array($data)) {
      throw new \InvalidArgumentException(
        sprintf(
          'Queue item data should be an array, %s given.',
          gettype($data)
        )
      );
    }

    // @I Implement error handling modes for data validation
    //    type     : bug
    //    priority : low
    //    labels   : error-handling, export, queue
    if (empty($data['sync_id'])) {
      throw new \InvalidArgumentException(
        'The ID of the synchronization that defines the export must be given.'
      );
    }

    if (empty($data['entity_type_id'])) {
      throw new \InvalidArgumentException(
        'The type ID of the entity being exported must be given.'
      );
    }

    if (empty($data['entity_id'])) {
      throw new \InvalidArgumentException(
        'The ID of the entity being exported must be given.'
      );
    }
  }

}
