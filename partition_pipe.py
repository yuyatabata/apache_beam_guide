import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


durations = ['annual', 'biennial', 'perennial']


def by_duration(plant, num_partitions):
  return durations.index(plant['duration'])


def run():
  # まずパイプラインを作る
  p = beam.Pipeline(options=PipelineOptions())

  annuals, biennials, perennials = (
      p
      | 'Gardening plants' >> beam.Create([
          {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'},
          {'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'},
          {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'},
          {'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'},
          {'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'},
      ])
      | 'Partition' >> beam.Partition(by_duration, len(durations))
  )

  annuals | 'Annuals' >> beam.Map(lambda x: print('annual: {}'.format(x)))
  biennials | 'Biennials' >> beam.Map(
      lambda x: print('biennial: {}'.format(x)))
  perennials | 'Perennials' >> beam.Map(
      lambda x: print('perennial: {}'.format(x)))

if __name__ == '__main__':
    run()
