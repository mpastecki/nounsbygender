#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Copyright Marcin Pastecki
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import argparse
import logging
import apache_beam as beam
from apache_beam import coders
import sys
import re
import csv

def compositeData(word, polimorf):
   if word[0] in polimorf:
       poliinfo = polimorf.get(word[0])
       yield (word[0], word[1], poliinfo)

class CreatePCollectionOfKV(beam.DoFn):
   def process(self, element):
       word, baseform, part, noun_number, noun_gender, noun_type = element.split(';')
       yield (word, (baseform, part, noun_number, noun_gender, noun_type))

class StripHTMLTags(beam.DoFn):
    def process(self, element):
        yield re.sub('<[^<]+?>', '', element)

def run():
   reload(sys)
   sys.setdefaultencoding('utf8')

   # Command line arguments
   parser = argparse.ArgumentParser(description='Count nouns by gender')
   parser.add_argument('--input', required=True, help='Specify Local or Cloud Storage for input files')
   parser.add_argument('--polimorf', required=True, help='Specify Local or Cloud Storage for Polimorf Dictionary')
   parser.add_argument('--output', required=True, help='Specify Local or Cloud storage for output files')
   parser.add_argument('--bucket', required=False, help='Specify Cloud Storage bucket for output', default='none')
   parser.add_argument('--project',required=False, help='Specify Google Cloud project', default='none')
   group = parser.add_mutually_exclusive_group(required=True)
   group.add_argument('--DirectRunner',action='store_true')
   group.add_argument('--DataFlowRunner',action='store_true')

   opts = parser.parse_args()

   if opts.DirectRunner:
     runner='DirectRunner'
   if opts.DataFlowRunner:
     runner='DataFlowRunner'

   bucket = opts.bucket
   input = opts.input
   output = opts.output
   project = opts.project
   polimorf = opts.polimorf

   argv = [
     '--project={0}'.format(project),
     '--job_name=nounscountbygender',
     '--save_main_session',
     '--staging_location=gs://{0}/staging/'.format(bucket),
     '--temp_location=gs://{0}/staging/'.format(bucket),
     '--runner={0}'.format(runner)
   ]

   p = beam.Pipeline(argv=argv)

   output_file_prefix = 'nounsgender'
   output_file_suffix = '.csv'

   polimorfpc = (p
       | 'ReadPolimorfFromText' >> beam.io.ReadFromText(polimorf)
       | 'CreatePCollectionOfKV' >> beam.ParDo(CreatePCollectionOfKV()))

   wordinfo = (p
       | 'ReadFromText' >> beam.io.ReadFromText(input)
       | 'StripFromHTML' >> beam.ParDo(StripHTMLTags())
       | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall('[^\W_\d]+', x, re.U))
       | 'CountWords' >> beam.combiners.Count.PerElement()
       | 'AddPolimorfInfo' >> beam.FlatMap(lambda element, the_dict: compositeData(element, the_dict), beam.pvalue.AsDict(polimorfpc)))

   (wordinfo
       | 'GetGenderStats' >> beam.Map(lambda tuple: (tuple[2][3][:1], tuple[1]))
       | 'GroupThemAll' >> beam.GroupByKey()
       | 'Sum' >> beam.Map(lambda tuple: (tuple[0], sum(tuple[1])))
       | 'FormatResult' >> beam.Map(lambda tuple: u'{},{}'.format(tuple[0],tuple[1]))
       | 'WriteGenderStatsToFile' >> beam.io.WriteToText(file_path_prefix = '{}/{}'.format(output, output_file_prefix), file_name_suffix = output_file_suffix)
   )

   if runner == 'DataFlowRunner':
      p.run()
   else:
      p.run().wait_until_finish()
   logging.getLogger().setLevel(logging.INFO)

if __name__ == '__main__':
   run()
