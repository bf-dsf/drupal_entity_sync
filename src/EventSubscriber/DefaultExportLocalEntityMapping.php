<?php

namespace Drupal\entity_sync\EventSubscriber;

use Drupal\entity_sync\Export\Event\Events;
use Drupal\entity_sync\Export\Event\LocalEntityMappingEvent;
use Drupal\entity_sync\Export\EntityManagerInterface;

use Symfony\Component\EventDispatcher\EventSubscriberInterface;

/**
 * Builds the default remote ID mapping for export operations.
 */
class DefaultExportLocalEntityMapping implements EventSubscriberInterface {

  /**
   * {@inheritdoc}
   */
  public static function getSubscribedEvents() {
    $events = [
      Events::LOCAL_ENTITY_MAPPING => ['buildEntityMapping', 0],
    ];
    return $events;
  }

  /**
   * Builds the default local entity mapping.
   *
   * The default local entity mapping is defined in the synchronization
   * configuration object.
   *
   * @param \Drupal\entity_sync\Export\Event\LocalEntityMappingEvent $event
   *   The local entity mapping event.
   */
  public function buildEntityMapping(LocalEntityMappingEvent $event) {
    $entity_mapping = [];

    $sync = $event->getSync();
    $local_entity = $event->getLocalEntity();

    // Check if the field exists.
    $id_field_name = $sync->get('local_entity.remote_id_field');
    if (!$local_entity->hasField($id_field_name)) {
      return;
    }

    // If we don't have a value it's a create, if we do it's an update.
    $id_field = $local_entity->get($id_field_name);
    if ($id_field->isEmpty()) {
      $entity_mapping = [
        'action' => EntityManagerInterface::ACTION_CREATE,
        'client' => $sync->get('remote_resource.client'),
      ];
    }
    else {
      $entity_mapping = [
        'action' => EntityManagerInterface::ACTION_UPDATE,
        'client' => $sync->get('remote_resource.client'),
        'id' => $id_field->value,
      ];
    }

    $event->setEntityMapping($entity_mapping);
  }

}
