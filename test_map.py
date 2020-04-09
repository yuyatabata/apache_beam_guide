import unittest
from unittest import TestCase

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to


class TestMap(TestCase):

    def test_map(self):
        expected = [5, 3, 7, 7, 5]

        inputs = ['Alice', 'Bob', 'Cameron', 'Daniele', 'Ellen']

        with TestPipeline() as p:
            actual = (p
                      | beam.Create(inputs)
                      | beam.Map(lambda element: len(element)))

            assert_that(actual, equal_to(expected))
            
            print(expected)
            print(actual)
            print((equal_to(expected)))

if __name__=="__main__":
    unittest.main()