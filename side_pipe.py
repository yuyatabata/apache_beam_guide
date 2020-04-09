import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import pvalue

# 平均以上の文字数を持つ文字列をフィルタリング
class FilterMeanLengthFn(beam.DoFn):
  def __init__(self):
    pass

  def process(self, element, mean_word_length):
    if len(element) >= mean_word_length:
      yield element
  

def run():
  # まずパイプラインを作る
  p = beam.Pipeline(options=PipelineOptions())

  # 1.配列をパイプラインの入力に設定
  inputs = ["good morning.", "good afternoon.", "good evening."]


  mean_word_length = (inputs
                      | beam.Map(len)
                      | beam.CombineGlobally(beam.combiners.MeanCombineFn()))

  print(mean_word_length)

  p_mean_word_length = (p | 'avg word len' >> beam.Create(mean_word_length))

  output =  (p
             | 'read' >> beam.Create(inputs)
             | 'FilterMeanLength' >> beam.ParDo(FilterMeanLengthFn(), pvalue.AsSingleton(p_mean_word_length))
             | 'write to text' >> beam.io.WriteToText('./output.txt')
             )
  print(output)
  p.run().wait_until_finish()

if __name__ == '__main__':
    run()
