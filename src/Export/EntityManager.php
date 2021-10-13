<?php

namespace Drupal\entity_sync\Export;

use Drupal\entity_sync\Client\ClientFactory;
use Drupal\entity_sync\Config\ManagerInterface as ConfigManagerInterface;
use Drupal\entity_sync\Export\Event\Events;
use Drupal\entity_sync\Export\Event\LocalEntityMappingEvent;
use Drupal\entity_sync\EntityManagerBase;
use Drupal\entity_sync\StateManagerInterface;

use Drupal\Core\Config\ConfigFactoryInterface;
use Drupal\Core\Config\ImmutableConfig;
use Drupal\Core\Entity\ContentEntityInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;

use Psr\Log\LoggerInterface;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

/**
 * The default export manager.
 */
class EntityManager extends EntityManagerBase implements EntityManagerInterface {

  /**
   * The client factory.
   *
   * @var \Drupal\entity_sync\Client\ClientFactory
   */
  protected $clientFactory;

  /**
   * The config factory.
   *
   * @var \Drupal\Core\Config\ConfigFactoryInterface
   */
  protected $configFactory;

  /**
   * The Entity Sync configuration manager.
   *
   * @var \Drupal\entity_sync\Config\ManagerInterface
   */
  protected $configManager;

  /**
   * The entity type manager.
   *
   * @var \Drupal\Core\Entity\EntityTypeManagerInterface
   */
  protected $entityTypeManager;

  /**
   * The event dispatcher.
   *
   * @var \Symfony\Component\EventDispatcher\EventDispatcherInterface
   */
  protected $eventDispatcher;

  /**
   * The Entity Sync export field manager.
   *
   * @var \Drupal\entity_sync\Export\FieldManagerInterface
   */
  protected $fieldManager;

  /**
   * The Entity Sync state manager.
   *
   * @var \Drupal\entity_sync\StateManagerInterface
   */
  protected $stateManager;

  /**
   * The logger service.
   *
   * @var \Psr\Log\LoggerInterface
   */
  protected $logger;

  /**
   * Constructs a new EntityManager instance.
   *
   * @param \Drupal\entity_sync\Client\ClientFactory $client_factory
   *   The client factory.
   * @param \Drupal\Core\Config\ConfigFactoryInterface $config_factory
   *   The configuration factory.
   * @param \Drupal\entity_sync\Config\ManagerInterface $config_manager
   *   The Entity Sync configuration manager.
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager
   *   The entity type manager.
   * @param \Symfony\Component\EventDispatcher\EventDispatcherInterface $event_dispatcher
   *   The event dispatcher.
   * @param \Drupal\entity_sync\Export\FieldManagerInterface $field_manager
   *   The Entity Sync export field manager.
   * @param \Drupal\entity_sync\StateManagerInterface $state_manager
   *   The Entity Sync export field manager.
   * @param \Psr\Log\LoggerInterface $logger
   *   The logger.
   */
  public function __construct(
    ClientFactory $client_factory,
    ConfigFactoryInterface $config_factory,
    ConfigManagerInterface $config_manager,
    EntityTypeManagerInterface $entity_type_manager,
    EventDispatcherInterface $event_dispatcher,
    FieldManagerInterface $field_manager,
    StateManagerInterface $state_manager,
    LoggerInterface $logger
  ) {
    $this->clientFactory = $client_factory;
    $this->configFactory = $config_factory;
    $this->configManager = $config_manager;
    $this->entityTypeManager = $entity_type_manager;
    $this->eventDispatcher = $event_dispatcher;
    $this->fieldManager = $field_manager;
    $this->stateManager = $state_manager;
    $this->logger = $logger;
  }

  /**
   * {@inheritDoc}
   */
  public function exportLocalEntity(
    $sync_id,
    ContentEntityInterface $local_entity,
    array $options = []
  ) {
    // Load the sync.
    // @I Validate the sync/operation
    //    type     : bug
    //    priority : normal
    //    labels   : operation, sync, validation
    //    notes    : Review whether the validation should happen upon runtime
    //               i.e. here, or when the configuration is created/imported.
    // @I Validate that the provider supports the `export_entity` operation
    //    type     : bug
    //    priority : normal
    //    labels   : operation, sync, validation
    //    notes    : Review whether the validation should happen upon runtime
    //               i.e. here, or when the configuration is created/imported.
    // @I Validate that the given local entity is of the correct type
    //    type     : bug
    //    priority : normal
    //    labels   : export, operation, sync, validation
    $sync = $this->configFactory->get('entity_sync.sync.' . $sync_id);

    // Make sure the operation is enabled and supported by the provider.
    if (!$this->operationSupported($sync, 'export_entity')) {
      $this->logger->error(
        sprintf(
          'The synchronization with ID "%s" and/or its provider do not support the `export_entity` operation.',
          $sync_id
        )
      );
      return;
    }

    $context = $options['context'] ?? [];

    // Notify subscribers that the operation is about to be initiated.
    // @I Write tests for operation cancellations
    //    type     : task
    //    priority : high
    //    labels   : export, testing
    $cancel = $this->preInitiate(
      Events::LOCAL_ENTITY_PRE_INITIATE,
      'export_entity',
      $context + ['local_entity' => $local_entity],
      $sync
    );
    if ($cancel) {
      return;
    }

    // Run the operation.
    // We do this in a `try/finally` structure so that we can still dispatch the
    // post-terminate event. Subscribers may still need to run whether the
    // operation was successfull or not. For example, even if a managed
    // operation failed we unlock it so that the next one is allowed to run.
    // At the end, the error/exception is still thrown so that the caller can
    // handle it as required.
    try {
      $this->doExportLocalEntity($sync, $local_entity, $options, $context);
    }
    finally {
      // Notify subscribers that the operation has terminated.
      $this->postTerminate(
        Events::LOCAL_ENTITY_POST_TERMINATE,
        'export_entity',
        $context,
        $sync
      );
    }
  }

  /**
   * {@inheritdoc}
   */
  public function queueExportLocalEntityAllSyncs(
    ContentEntityInterface $entity,
    array $options = []
  ) {
    // Get all syncs that define exports for the given entity's type.
    // @I Support caching syncs per entity type
    //    type     : improvement
    //    priority : normal
    //    labels   : export, performance
    //
    $entityTypeId = $entity->getEntityTypeId();
    $entityBundle = $entity->bundle();
    $localEntityParams = [
      'type_id' => $entityTypeId,
    ];

    if ($entityBundle && $entityTypeId !== $entityBundle) {
      $localEntityParams['bundle'] = $entityBundle;
    }

    $syncs = $this->configManager->getSyncs([
      'status' => TRUE,
      'local_entity' => $localEntityParams,
      'operation' => [
        'id' => 'export_entity',
        'status' => TRUE,
      ],
    ]);
    if (!$syncs) {
      return;
    }

    // @I Validate the original entity passed through context
    //    type     : bug
    //    priority : low
    //    labels   : export, validation
    $context = $options['context'] ?? [];
    // The most common case is to call this method in a insert/update hook. At
    // that point the entity has already been saved and the entity's `isNew()`
    // method will return `FALSE`. We therefore expect to be given the original
    // entity and use that to detect we are responding to a local entity
    // creation or update.
    $is_update = isset($context['original_entity']) ? TRUE : FALSE;
    $changed_field_names = [];

    // Detect whether we have changed fields. If the operation is managed by
    // Entity Sync for a given synchronization and there are not changed fields,
    // we will be skipping the export.
    //
    // @I Support forcing an export even if unchanged via config export mode
    //    type     : improvement
    //    priority : low
    //    labels   : config, export
    if ($is_update) {
      $changed_field_names = $this->fieldManager->getChangedNames(
        $entity,
        $context['original_entity']
      );
    }

    foreach ($syncs as $sync) {
      $is_managed = $this->stateManager->isManaged(
        $sync->get('id'),
        'export_entity'
      );

      // Do not proceed with queueing the export if we don't have any changed
      // fields that are also set to be exported by the current synchronization.
      if ($is_managed && $is_update) {
        if (!$changed_field_names) {
          continue;
        }

        $exportable_field_names = $this->fieldManager
          ->getExportableChangedNames(
            $entity,
            $context['original_entity'],
            $sync->get('field_mapping'),
            NULL,
            $changed_field_names
          );
        if (!$exportable_field_names) {
          continue;
        }
      }

      // If we are coming from an entity update, we may be here because an
      // entity import triggered an entity update hook. In that case, we
      // don't want to export the entity as that can cause a loop i.e. export
      // the entity to the remote which may cause the entity to be made
      // available in the recently modified list of entities, causing an import
      // again which in turn will trigger a `hook_entity_update` etc.
      //
      // If we are here as a result of a change that happened within Drupal e.g.
      // an entity edited via the UI, the remote changed field would not be
      // changed. While, if we are here as a result of an import, the original
      // remote changed field would be a timestamp earlier than the new remote
      // changed field.
      //
      // We therefore check for that, and if the original remote changed field
      // is earlier than the updated remote changed field then we do not export
      // the entity.
      //
      // This will also prevent triggering another export as a result of saving
      // the entity in order to update the remote changed field when getting
      // the response from the create/update export operation.
      //
      // It does not cover though the case that we import changes that have
      // already been imported where the remote changed field will be the same
      // before and after the import. That still remains to be resolved and it
      // relies on finding a way to let the method that saves the entity provide
      // custom data to `hook_entity_insert`.
      // See https://www.drupal.org/project/drupal/issues/3158180
      //
      // Note that we may not always have a remote changed field; that can be,
      // for example, when we only export an entity type while imports are
      // disabled. We therefore skip the check if imports are disabled.
      //
      // @I Test skipping exports that are triggered by imports
      //    type     : task
      //    priority : high
      //    labels   : export, testing
      // @I Review case of imports+exports without a remote changed field
      //    type     : bug
      //    priority : high
      //    labels   : export
      $import_status = FALSE;
      if ($sync->get('operations.import_list.status')) {
        $import_status = TRUE;
      }
      elseif ($sync->get('operations.import_entity.status')) {
        $import_status = TRUE;
      }
      if ($import_status && $is_managed && $is_update) {
        $field_name = $sync->get('local_entity.remote_changed_field');

        $original_changed = $context['original_entity']
          ->get($field_name)
          ->value;
        if ($original_changed < $entity->get($field_name)->value) {
          continue;
        }
      }

      // If we are coming here from an entity creation, we want to prevent
      // exporting entities that were created as a result of an import. In those
      // cases, the remote changed field of the new local entity will not be
      // empty, while if the entity was created within the application it will
      // always be empty.
      // See entity_sync_entity_bundle_field_info_alter().
      //
      // As above, we skip the checks if imports are disabled.
      //
      // @I Allow queueing exports of imported entities for different syncs
      //    type     : bug
      //    priority : high
      //    labels   : export
      //    notes    : There are legitimate cases where we still want to allow
      //               exports of new imported entities e.g. import from one
      //               remote resource and send to another.
      elseif ($import_status && $is_managed && !$is_update) {
        $remote_changed_name = $sync->get('local_entity.remote_changed_field');
        $remote_changed_field = $entity->get(
          $sync->get('local_entity.remote_changed_field')
        );

        if (!$remote_changed_field->isEmpty()) {
          continue;
        }
      }

      // Queue the export.
      // @I Support exporting the diff between specific revisions
      //    type     : improvement
      //    priority : normal
      //    labels   : export
      //    notes    : If the entity being exported is revisioned, we can store
      //               in the queue item the specific revisions and export the
      //               fields changed between those revisions.
      $queue = \Drupal::queue('entity_sync_export_local_entity');
      $queue->createItem([
        'sync_id' => $sync->get('id'),
        'entity_type_id' => $entity->getEntityTypeId(),
        'entity_id' => $entity->id(),
      ]);
    }
  }

  /**
   * Runs the actual local entity export operation.
   *
   * @param \Drupal\Core\Config\ImmutableConfig $sync
   *   The configuration object for synchronization that defines the operation
   *   we are currently executing.
   * @param \Drupal\Core\Entity\ContentEntityInterface $local_entity
   *   The local entity.
   * @param array $options
   *   An associative array of options that determine various aspects of the
   *   export. For supported options see
   *   \Drupal\entity_sync\Export\EntityManagerInterface::exportLocalEntity().
   * @param array $context
   *   An associative array containing context related to the circumstances of
   *   the operation. See
   *   \Drupal\entity_sync\Export\ManagerInterface::exportLocalEntity().
   */
  protected function doExportLocalEntity(
    ImmutableConfig $sync,
    ContentEntityInterface $local_entity,
    array $options,
    array $context
  ) {
    // Initiate the operation.
    $this->initiate(
      Events::LOCAL_ENTITY_INITIATE,
      'export_entity',
      $context,
      $sync
    );

    // Export the entity.
    $data = $this->createOrUpdate($local_entity, $sync);

    // Terminate the operation.
    // Add to the context the local entity that was exported.
    $this->terminate(
      Events::LOCAL_ENTITY_TERMINATE,
      'export_entity',
      $context + ['local_entity' => $local_entity],
      $sync,
      $data ?? []
    );
  }

  /**
   * Export the changes contained in the given local entity to a remote entity.
   *
   * If an associated remote entity is identified, the remote entity will be
   * updated. A new remote entity will be created otherwise.
   *
   * @param \Drupal\Core\Entity\ContentEntityInterface $local_entity
   *   The local entity.
   * @param \Drupal\Core\Config\ImmutableConfig $sync
   *   The configuration object for synchronization that defines the operation
   *   we are currently executing.
   */
  protected function createOrUpdate(
    ContentEntityInterface $local_entity,
    ImmutableConfig $sync
  ) {
    // Build the entity mapping for this local entity.
    $entity_mapping = $this->localEntityMapping($local_entity, $sync);

    // If the entity mapping is empty we will not be updating or creating a
    // remote entity; nothing to do.
    if (!$entity_mapping) {
      return;
    }

    $data = ['action' => $entity_mapping['action']];

    // Skip exporting the local entity if we are explicitly told to do so.
    if ($entity_mapping['action'] === EntityManagerInterface::ACTION_SKIP) {
      return $data;
    }
    elseif ($entity_mapping['action'] === EntityManagerInterface::ACTION_CREATE) {
      $data['response'] = $this->create($local_entity, $sync, $entity_mapping);
      return $data;
    }
    elseif ($entity_mapping['action'] === EntityManagerInterface::ACTION_UPDATE) {
      $data['response'] = $this->update($local_entity, $sync, $entity_mapping);
      return $data;
    }
    else {
      throw new \RuntimeException(
        sprintf(
          'Unsupported entity mapping action "%s"',
          $entity_mapping['action']
        )
      );
    }
  }

  /**
   * Export the changes from the given local entity to a new remote entity.
   *
   * @param \Drupal\Core\Entity\ContentEntityInterface $local_entity
   *   The local entity.
   * @param \Drupal\Core\Config\ImmutableConfig $sync
   *   The configuration object for synchronization that defines the operation
   *   we are currently executing.
   * @param array $entity_mapping
   *   An associative array containing information about the remote entity being
   *   mapped to the given local entity.
   *   See \Drupal\entity_sync\Export\Event\LocalEntityMapping::entityMapping.
   *
   * @return mixed|null
   *   The response from the client, or NULL if the operation was not run.
   */
  protected function create(
    ContentEntityInterface $local_entity,
    ImmutableConfig $sync,
    array $entity_mapping
  ) {
    if (!$sync->get('operations.export_entity.create_entities')) {
      return;
    }

    // Prepare the fields to create on the remote object.
    // @I Pass context to the field manager
    //    type     : improvement
    //    priority : normal
    //    labels   : field, export
    $remote_fields = $this->fieldManager->export(
      $local_entity,
      NULL,
      $sync
    );

    // Do the export i.e. call the client.
    return $this->clientFactory
      ->getByClientConfig($entity_mapping['client'])
      ->create($remote_fields);
  }

  /**
   * Export the changes from the given local entity to the remote entity.
   *
   * @param \Drupal\Core\Entity\ContentEntityInterface $local_entity
   *   The local entity.
   * @param \Drupal\Core\Config\ImmutableConfig $sync
   *   The configuration object for synchronization that defines the operation
   *   we are currently executing.
   * @param array $entity_mapping
   *   An associative array containing information about the remote entity being
   *   mapped to the given local entity.
   *   See \Drupal\entity_sync\Export\Event\LocalEntityMapping::entityMapping.
   *
   * @return mixed|null
   *   The response from the client, or NULL if the operation was not run.
   *
   * @I Support entity update validation
   *    type     : bug
   *    priority : normal
   *    labels   : export, validation
   * @I Check if the changes have already been exported
   *    type     : improvement
   *    priority : normal
   *    labels   : export, validation
   */
  protected function update(
    ContentEntityInterface $local_entity,
    ImmutableConfig $sync,
    array $entity_mapping
  ) {
    if (!$sync->get('operations.export_entity.update_entities')) {
      return;
    }

    // Prepare the fields to update on the remote object.
    // @I Pass context to the field manager
    //    type     : improvement
    //    priority : normal
    //    labels   : field, export
    $remote_fields = $this->fieldManager->export(
      $local_entity,
      $entity_mapping['id'],
      $sync
    );

    // Do the export i.e. call the client.
    return $this->clientFactory
      ->getByClientConfig($entity_mapping['client'])
      ->update($entity_mapping['id'], $remote_fields);
  }

  /**
   * Builds and returns the remote ID for the given local entity.
   *
   * The local entity mapping defines if and which remote entity this local
   * entity will be exported to. The default mapping identifies the remote
   * entity based on a local entity field containing the remote entity's ID.
   *
   * An event is dispatched that allows subscribers to map the local entity to a
   * different remote entity, or to decide to not export it at all.
   *
   * @param \Drupal\core\Entity\ContentEntityInterface $local_entity
   *   The local entity.
   * @param \Drupal\Core\Config\ImmutableConfig $sync
   *   The configuration object for synchronization that defines the operation
   *   we are currently executing.
   *
   * @return array
   *   The final entity mapping.
   */
  protected function localEntityMapping(
    ContentEntityInterface $local_entity,
    ImmutableConfig $sync
  ) {
    $event = new LocalEntityMappingEvent(
      $local_entity,
      $sync
    );
    $this->eventDispatcher->dispatch(Events::LOCAL_ENTITY_MAPPING, $event);

    // Return the final mapping.
    return $event->getEntityMapping();
  }

}
