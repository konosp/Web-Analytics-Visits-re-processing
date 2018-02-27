from __future__ import absolute_import

import argparse
import csv
import logging
import sys
import pdb

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.transforms import window

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# [START main]
def run(argv=None):
    """Main entry point; defines and runs the visitor analysis pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='data/sample.tsv',
                        #default='gs://feeddata-test-konos-1/tmp/encoded*',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        # CHANGE 1/5: The Google Cloud Storage path is required
                        # for outputting the results.
                        default='data/visits.csv',
                        #default='gs://feeddata-test-konos-1/visit-analysis/visits.csv',
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
        # run your pipeline on the Google Cloud Dataflow Service.
        #'--runner=DataflowRunner',
        # CHANGE 3/5: Your project ID is required in order to run your pipeline on
        # the Google Cloud Dataflow Service.
        '--project=test-r-big-query',
        # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
        # files.
        '--staging_location=gs://feeddata-test-konos-1/visitor/staging/',
        # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
        # files.
        '--temp_location=gs://feeddata-test-konos-1/visitor/tmp/',
        '--job_name=visitoranalysis2',
    ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
  
    class filter_product_views(beam.DoFn):

        def process(self, element):
            try:
                #row = element.encode('utf-8')
                #row = element.decode('ISO-8859-1') #.decode('unicode-escape')
                columns = element.split('\t')
                timestamp = columns[0]
                user_id = columns[1] + '_' + columns[2]
                products_string = columns[4]
                line_number = ''
                if (not products_string == ''):
                    line_number = products_string.split(';')[1]
                
                # events = columns[5]
                res = [
                        timestamp,
                        user_id,
                        line_number,
                        1
                        #'events': events.split(',')
                ]
                #pdb.set_trace()
                yield res
            except:
                # Do nothing, discard the line
                print('Error')

    class AddTimestampDoFn(beam.DoFn):

        def process(self, element):
            # Extract the numeric Unix seconds-since-epoch timestamp to be
            # associated with the current log entry.
            if (len(element) > 0):
                timestamp = element.pop(0)
                user_id = element.pop(0)
                if (not timestamp == ''):
                    unix_timestamp = int(timestamp)
                    # new_element = [element[1], element[0]]
                    # new_element = [user_id, {
                    #    'ts' :unix_timestamp, 
                    #    'data':element}
                    #    ]
                    new_element = [user_id, timestamp]
                    # Wrap and emit the current entry and new timestamp in a
                    # TimestampedValue.
                    # pdb.set_trace()
                    yield beam.window.TimestampedValue(new_element, unix_timestamp)

    class reformat_int_csv(beam.DoFn):
        def process (self, element):
            return {element[0] + ',' + min(element[1])}

    session_timeout_seconds = int(60 * 30)

    with beam.Pipeline(options=pipeline_options) as p:
        data = (
            p | 'Read data' >> ReadFromText(known_args.input)
            | 'Filter' >> beam.ParDo(filter_product_views())
            | 'Add timestamp' >> beam.ParDo(AddTimestampDoFn())
            | 'Re-assess Sessions' >> beam.WindowInto(window.Sessions(session_timeout_seconds))
            | 'Combine' >> beam.GroupByKey()
            | 'Format CSV' >> beam.ParDo(reformat_int_csv())
            )
        data | 'Generate output' >>  WriteToText(known_args.output)
    # [END main]
if __name__ == '__main__':
  # logging.getLogger().setLevel(logging.INFO)
  run()