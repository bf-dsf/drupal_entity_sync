<?php

namespace Drupal\entity_sync\EventSubscriber;

use Drupal\entity_sync\Export\Event\Events;
use Drupal\entity_sync\Event\TerminateOperationEvent;
use Drupal\entity_sync\Export\EntityManagerInterface;
use Drupal\entity_sync\Import\FieldManagerInterface;

use Symfony\Component\EventDispatcher\EventSubscriberInterface;

/**
 * Updates the local entity after the export local entity terminate event.
 */
class ManagedExportLocalEntityTerminate implements EventSubscriberInterface {

  /**
   * The Entity Sync import field manager.
   *
   * @var \Drupal\entity_sync\Import\FieldManagerInterface
   */
  protected $fieldManager;

  /**
   * Constructs a new ManagedExportLocalEntityTerminate object.
   *
   * @param \Drupal\entity_sync\Import\FieldManagerInterface $field_manager
   *   The import field manager.
   */
  public function __construct(FieldManagerInterface $field_manager) {
    $this->fieldManager = $field_manager;
  }

  /**
   * {@inheritdoc}
   */
  public static function getSubscribedEvents() {
    $events = [
      Events::LOCAL_ENTITY_TERMINATE => ['updateLocalEntity', 0],
    ];
    return $events;
  }

  /**
   * Update the remote ID and remote changed fields on the local entity.
   *
   * The remote changed field is updated for both new and updated remote
   * entities.
   *
   * The remote ID is only updated when new remote entities are created. We do
   * check however that the field does not already have value - normally it
   * shouldn't, but in rare configurations we may be creating a new remote
   * entity as a result of a local entity update in which case we may have a
   * remote ID already set.
   *
   * @param \Drupal\entity_sync\Event\TerminateOperationEvent $event
   *   The terminate operation event.
   *
   * @I Do not set the sync fields if the entity was not created or updated
   *    type     : bug
   *    priority : high
   *    labels   : export
   *    notes    : In cases such as when the sync has entity creates or updates
   *               disabled we never make a request to the remote and therefore
   *               don't have a response available. The error thrown here in
   *               that case might block other terminate subscribers from being
   *               run.
   */
  public function updateLocalEntity(TerminateOperationEvent $event) {
    $data = $event->getData();

    $actions = [
      EntityManagerInterface::ACTION_CREATE,
      EntityManagerInterface::ACTION_UPDATE,
    ];
    if (!in_array($data['action'], $actions, TRUE)) {
      return;
    }

    $sync = $event->getSync();
    $local_entity = $event->getContext()['local_entity'];
    $local_entity_changed = FALSE;

    // We only set the remote changed field if it is defined in the
    // synchronization configuration. There are cases where it is not, such as
    // when we send the entity to a remote resource when created or updated but
    // we do not track an association.
    if ($sync->get('remote_resource.changed_field')) {
      $this->fieldManager->setRemoteChangedField(
        $data['response'],
        $local_entity,
        $event->getSync()
      );
      $local_entity_changed = TRUE;
    }

    // Similarly, we only set the remote ID field if it is defined in the
    // synchronization configuration. If it is, by default we only set it if we
    // are creating a new remote ID; when are updating an existing one we should
    // already have the association correctly stored in the field, in the most
    // common import/export flows. Custom flows might have to create custom
    // subscribers to define the desired behavior.
    $is_create = $data['action'] === EntityManagerInterface::ACTION_CREATE;
    if ($is_create && $sync->get('remote_resource.id_field')) {
      $this->fieldManager->setRemoteIdField(
        $data['response'],
        $local_entity,
        $event->getSync(),
        // Only if it does not already have a value.
        FALSE
      );
      $local_entity_changed = TRUE;
    }

    if (!$local_entity_changed) {
      return;
    }

    $local_entity->save();
  }

}
