<?php

namespace Drupal\entity_sync\Config;

use Drupal\Component\Utility\NestedArray;
use Drupal\Core\Config\ConfigFactoryInterface;

/**
 * Default implementation of the Entity Sync configuration manager.
 *
 * @I Rename to `SyncManager` to separate from `ProviderManager`
 *    type     : task
 *    priority : low
 *    labels   : config, structure
 */
class Manager implements ManagerInterface {

  /**
   * The config factory.
   *
   * @var \Drupal\Core\Config\ConfigFactoryInterface
   */
  protected $configFactory;

  /**
   * Constructs a new Manager instance.
   *
   * @param \Drupal\Core\Config\ConfigFactoryInterface $config_factory
   *   The configuration factory.
   */
  public function __construct(ConfigFactoryInterface $config_factory) {
    $this->configFactory = $config_factory;
  }

  /**
   * {@inheritdoc}
   *
   * @I Rewrite the `getSyncs` method so that it is more performant
   *    type     : task
   *    priority : low
   *    labels   : config, performance
   *    notes    : We use multiple array reduce/filter functions because it is
   *               easier to follow the logic. However, the function ends up
   *               being too long and that makes the readability benefits
   *               questionable so the performance hit is not justified. Review
   *               implementation.
   */
  public function getSyncs(array $filters = []) {
    $filters = NestedArray::mergeDeep(
      [
        'status' => NULL,
        'local_entity' => ['type_id' => NULL],
        'operation' => [
          'id' => NULL,
          'status' => NULL,
        ],
      ],
      $filters
    );

    $sync_ids = $this->configFactory->listAll('entity_sync.sync.');
    if (!$sync_ids) {
      return [];
    }

    // Load all synchronization configurations from the database.
    $syncs = array_reduce(
      $sync_ids,
      function ($carry, $sync_id) {
        $sync = $this->configFactory->get($sync_id);
        $carry[$sync->get('id')] = $sync;
        return $carry;
      },
      []
    );

    // Filter out any synchronization that do not match bundle
    $syncs = array_filter(
      $syncs,
      static function ($sync) use ($filters) {
        if (isset($filters['local_entity']['bundle'])) {
          $sync_bundle = $sync->get('local_entity.bundle');
          return ($sync_bundle && $sync_bundle === $filters['local_entity']['bundle']);
        }
        return TRUE;
      }
    );

    // Filter out any synchronization that do not match the status, if given.
    $syncs = array_filter(
      $syncs,
      static function ($sync) use ($filters) {
        if ($filters['status'] === NULL) {
          return TRUE;
        }

        if ($sync->get('status') !== $filters['status']) {
          return FALSE;
        }

        return TRUE;
      }
    );

    // Filter out any synchronization that do not match the entity type ID
    // filter, if given.
    $syncs = array_filter(
      $syncs,
      static function ($sync) use ($filters) {
        if (!$filters['local_entity']['type_id']) {
          return TRUE;
        }

        if ($sync->get('local_entity.type_id') !== $filters['local_entity']['type_id']) {
          return FALSE;
        }

        return TRUE;
      }
    );

    // Filter out any synchronization that do not match the operation ID and
    // status, if given.
    return array_filter(
      $syncs,
      static function ($sync) use ($filters) {
        if (!$filters['operation']['id']) {
          return TRUE;
        }

        $operation = $sync->get('operations.' . $filters['operation']['id']);
        if (!$operation) {
          return FALSE;
        }

        if ($filters['operation']['status'] === NULL) {
          return TRUE;
        }

        if ($operation['status'] !== $filters['operation']['status']) {
          return FALSE;
        }

        return TRUE;
      }
    );
  }

  /**
   * {@inheritdoc}
   */
  public function mergeFieldMappingDefaults(array $config) {
    return NestedArray::mergeDeep(
      $this->fieldMappingDefaults(),
      $config
    );
  }

  /**
   * {@inheritdoc}
   */
  public function mergeExportFieldMappingDefaults(array $config) {
    return NestedArray::mergeDeep(
      $this->exportFieldMappingDefaults(),
      $config
    );
  }

  /**
   * {@inheritdoc}
   */
  public function mergeImportFieldMappingDefaults(array $config) {
    return NestedArray::mergeDeep(
      $this->importFieldMappingDefaults(),
      $config
    );
  }

  /**
   * {@inheritdoc}
   */
  public function fieldMappingDefaults() {
    return $this->exportFieldMappingDefaults() +
      $this->importFieldMappingDefaults();
  }

  /**
   * {@inheritdoc}
   */
  public function exportFieldMappingDefaults() {
    return [
      'export' => [
        'status' => TRUE,
        'callback' => [],
      ],
    ];
  }

  /**
   * {@inheritdoc}
   */
  public function importFieldMappingDefaults() {
    return [
      'import' => [
        'status' => TRUE,
        'callback' => [],
      ],
    ];
  }

}
