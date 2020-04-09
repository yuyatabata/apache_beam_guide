import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import pvalue

# 平均以上の文字数を持つ文字列をフィルタリング
class ComputeWordCount(beam.PTransform):
  def __init__(self):
    pass

  def expand(self, pcoll):
    return (pcoll
            | 'SplitHalfSpace' >> beam.Map(lambda element: element.split(' ')) 
            | 'ComputeArraySize' >> beam.Map(lambda element: len(element)))
  

def run():
  # まずパイプラインを作る
  p = beam.Pipeline(options=PipelineOptions())

  # 1.配列をパイプラインの入力に設定
  inputs = ['There is no time like the present.', 'Time is money.']

  (p
   | 'read' >> beam.Create(inputs)
   | 'transform' >> ComputeWordCount()
   | 'write' >> beam.io.WriteToText('./output_composite.txt'))

  p.run()

if __name__ == '__main__':
    run()
