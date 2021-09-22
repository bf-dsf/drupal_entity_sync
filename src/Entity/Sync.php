<?php

namespace Drupal\entity_sync\Entity;

use Drupal\Core\Config\Entity\ConfigEntityBase;

/**
 * Defines the default Synchronization config entity class.
 *
 * @ConfigEntityType(
 *   id = "entity_sync",
 *   label = @Translation("Entity synchronization"),
 *   label_collection = @Translation("Entity synchronizations"),
 *   label_singular = @Translation("entity synchronization"),
 *   label_plural = @Translation("entity synchronizations"),
 *   label_count = @PluralTranslation(
 *     singular = "@count entity synchronization",
 *     plural = "@count entity synchronizations",
 *   ),
 *   config_prefix = "sync",
 *   config_export = {
 *     "id",
 *     "label",
 *     "uuid",
 *     "config"
 *   },
 *   entity_keys = {
 *     "id" = "id",
 *     "label" = "label",
 *     "uuid" = "uuid"
 *   }
 * )
 */
class Sync extends ConfigEntityBase implements SyncInterface {

  /**
   * The synchronization ID.
   *
   * @var string
   */
  protected $id;

  /**
   * The synchronization label.
   *
   * @var string
   */
  protected $label;

  /**
   * The synchronization description.
   *
   * @var string
   */
  protected $description;

  /**
   * The local entity settings.
   *
   * @var array
   */
  protected $local_entity;

  /**
   * The remote resource settings.
   *
   * @var array
   */
  protected $remote_resource;

  /**
   * The operations settings.
   *
   * @var array
   */
  protected $operations;

  /**
   * The field mapping settings.
   *
   * @var array
   */
  protected $field_mapping;

  /**
   * {@inheritdoc}
   */
  public function getDescription() {
    return $this->description;
  }

  /**
   * {@inheritdoc}
   */
  public function setDescription($description) {
    $this->description = $description;
    return $this;
  }

  /**
   * {@inheritdoc}
   */
  public function getLocalEntitySettings() {
    return $this->local_entity;
  }

  /**
   * {@inheritdoc}
   */
  public function getRemoteResourceSettings() {
    return $this->remote_resource;
  }

  /**
   * {@inheritdoc}
   */
  public function getOperationsSettings() {
    return $this->operations;
  }

  /**
   * {@inheritdoc}
   */
  public function getFieldMapping() {
    return $this->field_mapping;
  }

}
