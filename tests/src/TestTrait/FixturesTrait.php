<?php

namespace Drupal\Tests\entity_sync\TestTrait;

use Drupal\Component\Utility\NestedArray;
use Symfony\Component\Yaml\Yaml;

/**
 * Provides methods for making it easier to work with fixtures.
 */
trait FixturesTrait {

  /**
   * Gets the requested property value for the given fixture case.
   *
   * Used the configuration getter method as a starting point and largely
   * follows the same logic.
   *
   * @param string $key
   *   A string that maps to a key within the fixture data.
   *   For instance in the following data array:
   *   @code
   *   array(
   *     'foo' => array(
   *       'bar' => 'baz',
   *     ),
   *   );
   *   @endcode
   *   A key of 'foo.bar' would return the string 'baz'. However, a key of 'foo'
   *   would return array('bar' => 'baz').
   * @param string $case_id
   *   The ID of the fixture case from which to load the property.
   * @param string|null $fixture_id
   *   The ID of the fixture from which to load the case, or NULL for using the
   *   default. The default fixture ID must be defined by implementing the
   *   `defaultFixtureId()` method.
   * @param string|null $fixtures_directory
   *   The full path to the directory that contains the fixtures, without
   *   trailing slash.
   *
   * @return array|null
   *   The data for the requested key, or NULL if they don't exist.
   *
   * @throws \InvalidArgumentException
   *   When an empty key is provided.
   * @throws \InvalidArgumentException
   *   When an empty case ID is provided.
   * @throws \InvalidArgumentException
   *   When no fixture ID is provided and no default is defined.
   * @throws \InvalidArgumentException
   *   When no fixture directory is provided and no default is defined.
   *
   * @see \Drupal\Core\Config\ConfigBase::get()
   */
  protected function getFixtureDataProperty(
    $key,
    $case_id,
    $fixture_id = NULL,
    $fixtures_directory = NULL
  ) {
    if (!$key) {
      throw new \InvalidArgumentException(
        'The key of the fixture data property to get must be provided.'
      );
    }

    $data = $this->getFixtureCaseData(
      $case_id,
      $fixture_id,
      $fixtures_directory
    );
    $parts = explode('.', $key);

    if (count($parts) === 1) {
      return isset($data[$key]) ? $data[$key] : NULL;
    }

    $key_exists = NULL;
    $value = NestedArray::getValue($data, $parts, $key_exists);

    return $key_exists ? $value : NULL;
  }

  /**
   * Loads and returns the requested fixture data for the given fixture case.
   *
   * Deprecated; provided for not breaking existing tests that are using it at
   * the moment the `getFixtureCaseData` method was introduced.
   *
   * @param string $case_id
   *   The ID of the fixture case from which to load the data.
   * @param string|null $fixture_id
   *   The ID of the fixture from which to load the case, or NULL for using the
   *   default. The default fixture ID must be defined by implementing the
   *   `defaultFixtureId()` method.
   * @param string|null $fixtures_directory
   *   The full path to the directory that contains the fixtures, without
   *   trailing slash.
   *
   * @return array
   *   The data for the requested case.
   *
   * @throws \InvalidArgumentException
   *   When an empty case ID is provided.
   * @throws \InvalidArgumentException
   *   When no fixture ID is provided and no default is defined.
   * @throws \RuntimeException
   *   When the file determined to contain the fixture data does not exist.
   * @throws \RuntimeException
   *   When the contents of the file containing the fixture data cannot not be
   *   loaded.
   * @throws \RuntimeException
   *   When the loaded fixture data does not contain the requested case ID.
   *
   * @I Replace `getFixtureData` with `getFixtureCaseData` in all tests
   *    type     : task
   *    priority : low
   *    labels   : testing
   */
  protected function getFixtureData(
    $case_id,
    $fixture_id = NULL,
    $fixtures_directory = NULL
  ) {
    return $this->getFixtureCaseData($case_id, $fixture_id, $fixtures_directory);
  }

  /**
   * Loads and returns the requested fixture data for all fixture cases.
   *
   * @param string|null $fixture_id
   *   The ID of the fixture from which to load the case, or NULL for using the
   *   default. The default fixture ID must be defined by implementing the
   *   `defaultFixtureId()` method.
   * @param string|null $fixtures_directory
   *   The full path to the directory that contains the fixtures, without
   *   trailing slash.
   *
   * @return array
   *   The data for all cases.
   *
   * @throws \InvalidArgumentException
   *   When no fixture ID is provided and no default is defined.
   * @throws \RuntimeException
   *   When the file determined to contain the fixture data does not exist.
   * @throws \RuntimeException
   *   When the contents of the file containing the fixture data cannot not be
   *   loaded.
   */
  protected function getFixtureAllData(
    $fixture_id = NULL,
    $fixtures_directory = NULL
  ) {
    $fixture_id = $this->getFixtureId($fixture_id);
    $fixtures_directory = $this->getFixturesDirectory($fixtures_directory);
    $filename = realpath($fixtures_directory . '/' . $fixture_id . '.yml');

    if (!file_exists($filename)) {
      throw new \InvalidArgumentException(
        sprintf(
          'The file determined to hold the fixture data "%s" does not exist. Make sure that you have correctly requested the fixture ID and fixture directory or determined the defaults, "%s" and "%s" given.',
          $filename,
          $fixture_id,
          $fixtures_directory
        )
      );
    }

    $file_contents = file_get_contents($filename);
    if ($file_contents === FALSE) {
      throw \RuntimeException(
        sprintf(
          'The contents of the "%s" file containing the fixture data could not be loaded.',
          $filename
        )
      );
    }

    return Yaml::parse($file_contents);
  }

  /**
   * Loads and returns the requested fixture data for the given fixture case.
   *
   * @param string $case_id
   *   The ID of the fixture case from which to load the data.
   * @param string|null $fixture_id
   *   The ID of the fixture from which to load the case, or NULL for using the
   *   default. The default fixture ID must be defined by implementing the
   *   `defaultFixtureId()` method.
   * @param string|null $fixtures_directory
   *   The full path to the directory that contains the fixtures, without
   *   trailing slash.
   *
   * @return array
   *   The data for the requested case.
   *
   * @throws \InvalidArgumentException
   *   When an empty case ID is provided.
   * @throws \InvalidArgumentException
   *   When no fixture ID is provided and no default is defined.
   * @throws \RuntimeException
   *   When the file determined to contain the fixture data does not exist.
   * @throws \RuntimeException
   *   When the contents of the file containing the fixture data cannot not be
   *   loaded.
   * @throws \RuntimeException
   *   When the loaded fixture data does not contain the requested case ID.
   */
  protected function getFixtureCaseData(
    $case_id,
    $fixture_id = NULL,
    $fixtures_directory = NULL
  ) {
    if (!$case_id) {
      throw new \InvalidArgumentException(
        'The ID of the fixture data case from which to get the property must be provided.'
      );
    }

    $data = $this->getFixtureAllData($fixture_id, $fixtures_directory);

    if (isset($data[$case_id])) {
      return $data[$case_id];
    }

    $fixture_id = $this->getFixtureId($fixture_id);
    $fixtures_directory = $this->getFixturesDirectory($fixtures_directory);
    $filename = realpath($fixtures_directory . '/' . $fixture_id . '.yml');
    throw new \RuntimeException(
      sprintf(
        'Fixture case with ID "%s" not found in the data for fixture "%s", file "%s"',
        $case_id,
        $fixture_id,
        $filename
      )
    );
  }

  /**
   * Determines and returns the fixture ID based on the given value.
   *
   * If a fixture ID is provided, it returns that. If it is not, it returns the
   * default defined by the `defaultFixtureId` method.
   *
   * @param string|null $fixture_id
   *   The ID of the fixture from which to load the case, or NULL for using the
   *   default. The default fixture ID must be defined by implementing the
   *   `defaultFixtureId()` method.
   *
   * @return string
   *   The fixture ID.
   *
   * @throws \InvalidArgumentException
   *   When no fixture ID is provided and no default is defined.
   */
  protected function getFixtureId($fixture_id = NULL) {
    if ($fixture_id) {
      return $fixture_id;
    }

    if (!method_exists($this, 'defaultFixtureId')) {
      throw new \InvalidArgumentException(
        'The ID of the fixture data from which to get the case must be provided, or a default must be defined.'
      );
    }

    return $this->defaultFixtureId();
  }

  /**
   * Determines and returns the directory that contains the fixture data.
   *
   * If a directory is provided, it returns that. If not, it returns the default
   * defined by the `defaultFixturesDirectory` method. If not default is
   * provided, the `tests/fixtures` directory under the Entity Synchronization
   * module is returned.
   *
   * @param string|null $fixtures_directory
   *   The full path to directory containing the fixture data files, or NULL for
   *   using the default. The default is `tests/fixtures` under the Entity
   *   Synchronization module directory, assumed to be within the `contrib`
   *   modules folder. The default can be overriden by implementing the
   *   `defaultFixturesDirectory()` method.
   *
   * @return string
   *   The fixture directory.
   */
  protected function getFixturesDirectory($fixtures_directory = NULL) {
    if ($fixtures_directory) {
      return $fixtures_directory;
    }

    if (method_exists($this, 'defaultFixturesDirectory')) {
      return $this->defaultFixturesDirectory();
    }

    return $this->root . '/modules/contrib/entity_sync/tests/fixtures';
  }

}
