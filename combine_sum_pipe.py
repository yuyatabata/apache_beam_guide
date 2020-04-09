import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# 2.変換処理を設定
class AverageFn(beam.CombineFn):
  def create_accumulator(self):
    return (0.0, 0)

  def add_input(self, sum_count, input):
    (sum, count) = sum_count
    return sum + input, count + 1

  def merge_accumulators(self, accumulators):
    sums, counts = zip(*accumulators)
    return sum(sums), sum(counts)

  def extract_output(self, sum_count):
    (sum, count) = sum_count
    return sum / count if count else float('NaN')


def run():
  # まずパイプラインを作る
  p = beam.Pipeline(options=PipelineOptions())

  # 1.配列をパイプラインの入力に設定（４行を入力とする）
  pc = [1, 10, 100, 1000]

  output =  (p
             | 'read' >> beam.Create(pc)
             | 'transform' >> beam.CombineGlobally(sum).without_defaults()
             | 'write' >> beam.io.WriteToText('./output.txt'))

  p.run().wait_until_finish()
  # 3.最後に標準出力にカウント数を出力して終わる
#  average = pc | beam.CombineGlobally(AverageFn())

if __name__ == '__main__':
    run()
