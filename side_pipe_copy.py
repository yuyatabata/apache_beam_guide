import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import pvalue

words = ["good morning.", "good afternoon.", "good evening."]

def run():
  p = beam.Pipeline(options=PipelineOptions())
  # Callable takes additional arguments.
  def filter_using_length(word, lower_bound, upper_bound=float('inf')):
    if lower_bound <= len(word) <= upper_bound:
      yield word

  # Construct a deferred side input.
  avg_word_len = (
      words
      | beam.Map(len)
      | beam.CombineGlobally(beam.combiners.MeanCombineFn()))

  print(avg_word_len)

  p_avg_word_len = (p | 'avg word len' >> beam.Create(avg_word_len))


  # Call with explicit side inputs.
  small_words = words | 'small' >> beam.FlatMap(filter_using_length, 0, 3)

  # A single deferred side input.
  larger_than_average = (
      words | 'large' >> beam.FlatMap(
          filter_using_length, lower_bound=pvalue.AsSingleton(p_avg_word_len))
  )

  # Mix and match.
  small_but_nontrivial = words | beam.FlatMap(
      filter_using_length,
      lower_bound=2,
      upper_bound=pvalue.AsSingleton(avg_word_len))

if __name__ == '__main__':
    run()
