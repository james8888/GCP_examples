
from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class DataIngestion:

    def parse_method(self, string_input):
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))

        row = dict(zip(('empno', 'ename', 'sal', 'deptno'), values))

        return row


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input', dest='input', required=False,
        help='Input file to read.  This can be a local file or '
             'a file in a Google Storage Bucket.',
        default='gs://datafile/emp.csv')

    parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
                        default='lake.emp')

    parser.add_argument('--testfile', dest='testfile', required=False,
                        help='testfile',
                        default='gs://testfile/emp_output.csv')

    known_args, pipeline_args = parser.parse_known_args(argv)

    data_ingestion = DataIngestion()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))
    qry = 'select empno,ename,sal,deptno from `lake.emp`'

    BQ_DATA = p | 'read_bq_view' >> beam.io.Read(
               beam.io.BigQuerySource(query =  qry, use_standard_sql=True))
    BQ_VALUES = BQ_DATA | 'read values' >> beam.Map(lambda x: x.values())
    BQ_CSV = BQ_VALUES | 'CSV format' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
    BQ_CSV | 'Write_to_GCS' >> beam.io.WriteToText(
                        known_args.testfile,  file_name_suffix='.csv', header='empno,ename,sal,deptno')

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()