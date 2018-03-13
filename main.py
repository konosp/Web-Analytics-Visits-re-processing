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

class extract_data(beam.DoFn):

    def event_type(self, events, event_type):

        events_mapping = {
            'order' : '1',
            'pdp_view' : '2',
            'checkout' : '11',
            'atb' : '12',
            'rfb' : '13',
            'bag' : '14',
            'payment' : '204'
        }
        # Boolean outcome encoded in 0/1 so it can be summed up later on
        outcome = '0'
        if event_type in events_mapping:
            for event in events:
                if event == events_mapping[event_type]:
                    outcome = '1'

        return outcome

    def process(self, element):
        try:
            columns = element.split('\t')
            timestamp = columns[0]
            user_id = columns[1] + '_' + columns[2]
            tracking_code = columns[3]
            products_string = columns[4]
            page = columns[6]
            site_server = columns[7]
            ibm_id = str(element[8])
            line_number = ''
            if (not products_string == ''):
                line_number = products_string.split(';')[1]
            
            events = columns[5]
            events_list = events.split(',')
            res = {
                    'ts' : timestamp,
                    'user_id' : user_id,
                    'tracking_code' : tracking_code,
                    'line_number' : line_number,
                    'pdp_view': self.event_type(events_list,'pdp_view'),
                    'order' : self.event_type(events_list,'order'),
                    'bag_view': self.event_type(events_list, 'bag'),
                    'atb' : self.event_type(events_list, 'atb'),
                    'checkout' : self.event_type(events_list, 'checkout'),
                    'payment' : self.event_type(events_list, 'payment'),
                    'server' : site_server,
                    'page' : page
            }
            yield res
        except:
            # Do nothing, discard the line
            # TODO: Add error tracking through Stackdriver
            print('Error')

class AddTimestampDoFn(beam.DoFn):

    def process(self, element):
        # Extract the numeric Unix seconds-since-epoch timestamp to be
        # associated with the current log entry.
        if (len(element) > 0):
            timestamp = element['ts']
            user_id = element.pop('user_id')
            
            if (not timestamp == ''):
                unix_timestamp = int(timestamp)
                new_element = [user_id, element]
                # Wrap and emit the current entry and new timestamp in a
                # TimestampedValue.
                yield beam.window.TimestampedValue(new_element, unix_timestamp)

class reformat_into_csv_visits(beam.DoFn):
    def process (self, element):
        output = element['visit_key'] + ',' +  element['user_id'] + ',' +  element['visit_start'] + ',' +  element['visit_end']
        yield output

class reformat_into_csv_hits(beam.DoFn):
    def process (self, element):
        output = element['visit_key'] + ',' + element['ts'] + ',' +  element['server'] + ',' +  element['tracking_code'] + ',' + element['page'] + ',' + element['line_number'] + ',' + element['pdp_view'] + ',' +  element['atb'] + ',' + element['bag_view'] + ',' + element['checkout'] + ',' + element['payment'] + ',' + element['order']
        # output = element
        yield output

class calc_timestamps_group_hits_by_visit(beam.DoFn):
    def process (self, element):
        hits = element[1]
        user_id = element[0]
        timestamps = []
        for hit in hits:
            timestamps.append(hit['ts'])
        visit_start = min(timestamps)
        visit_end = max(timestamps)
        visit_key = str(user_id) + '_' + str(visit_start)
        #hits['visit_key'] = visit_key
        for hit in hits:
            hit['visit_key'] = visit_key
        data = {
            'user_id' : user_id,
            'visit_key' : visit_key,
            'visit_start' : visit_start,
            'visit_end' : visit_end,
            'hits' : hits
        }
        yield data

class extract_visit_data(beam.DoFn):
    def process (self, element):
        visit_only_values = {}
        for item in element:
            if item != 'hits':
                visit_only_values[item] = element[item]
        yield visit_only_values

class extract_hit_data(beam.DoFn):
    def process (self, element):
        #pdb.set_trace()
        yield element['hits']

class split_hits_into_lines(beam.DoFn):
    def process (self, element):
        return (element)

# [START main]
def run(argv=None):
    """Main entry point; defines and runs the visitor analysis pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='data/sample.tsv',
                        # default='gs://visit-analysis/raw-data/encoded_feeds/*',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        # CHANGE 1/5: The Google Cloud Storage path is required
                        # for outputting the results.
                        default='data/output/',
                        # default='gs://visit-analysis/new-visits/',
                        help='Output path to write results to.')
    parser.add_argument('--runner',
                        dest='runner',
                        default='DataflowRunner',
                        required='True',
                        help='DirectRunner or DataflowRunner')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
        # run your pipeline on the Google Cloud Dataflow Service.
        '--runner=' + known_args.runner,
        # CHANGE 3/5: Your project ID is required in order to run your pipeline on
        # the Google Cloud Dataflow Service.
        '--project=test-r-big-query',
        # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
        # files.
        '--staging_location=gs://feeddata-test-konos-1/visitor/staging/',
        # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
        # files.
        '--temp_location=gs://feeddata-test-konos-1/visitor/tmp/',
        '--job_name=visitor_analysis',
    ])
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    session_timeout_seconds = int(60 * 30)

    with beam.Pipeline(options=pipeline_options) as p:
        data = (p | 'Read data' >> ReadFromText(known_args.input)
            | 'Filter & Extract data' >> beam.ParDo(extract_data())
            | 'Add timestamp' >> beam.ParDo(AddTimestampDoFn())
            | 'Re-assess Sessions: ' + str(session_timeout_seconds) + ' seconds timeout' >> beam.WindowInto(window.Sessions(session_timeout_seconds))
            | 'Group data' >> beam.GroupByKey()
            | 'Calculate visit timestamps' >> beam.ParDo(calc_timestamps_group_hits_by_visit()))
        # Duplicate formated data into two streams for separate additional processing
        hit_data = data 
        visit_data = data
        # Start processing for hits/visits
        visit_data = visit_data | 'Extract Visit-related information' >> beam.ParDo(extract_visit_data())
        hit_data = hit_data | 'Extract Hit-related information' >> beam.ParDo(extract_hit_data())
        hit_data = hit_data | 'Split hits in multiple lines' >> beam.ParDo(split_hits_into_lines())
        visit_data = visit_data | 'Prepare final format - Visits' >> beam.ParDo(reformat_into_csv_visits())
        hit_data = hit_data | 'Prepare final format - Hits' >> beam.ParDo(reformat_into_csv_hits())
        visit_data | 'Generate output - Visits' >>  WriteToText(known_args.output + 'visits/visits.csv')
        hit_data | 'Generate output - Hits' >>  WriteToText(known_args.output + 'hits/hits.csv')
    # [END main]
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  # Run Forest, run!
  run()