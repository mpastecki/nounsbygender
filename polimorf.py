#!/usr/bin/env python

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
import sys
import re

class AddTypeOfWord(beam.DoFn):
    def process(self, element):
        if len(element.split('\t')) < 4:
            yield element + '\tunknown'
        else:
            yield element

class TransformPolimorf(beam.DoFn):
    def process(self, element):
        word, baseform, meta, noun_type = element.split('\t')

        part = ''
        noun_number = ''
        noun_gender = ''

        try:
            pattern = re.compile('^(?:[^:]*\:){0}([^:]*)')
            part = pattern.search(meta).group(1)
        except AttributeError:
            part = 'unknown'

        if part == 'subst' or part == 'depr':
            try:
                pattern = re.compile('^(?:[^:]*\:){1}([^:]*)')
                noun_number = pattern.search(meta).group(1)
            except AttributeError:
                noun_number = 'unknown'

            try:
                pattern = re.compile('^(?:[^:]*\:){3}([^:]*)')
                noun_gender = pattern.search(meta).group(1)
            except AttributeError:
                noun_gender = 'unknown'

        yield ('{};{};{};{};{};{}'.format(word, baseform, part, noun_number, noun_gender, noun_type))

class FilterNouns(beam.DoFn):
    def process(self, element):
        form = element.split(';')[2]
        if form == 'subst' or form == 'depr':
            yield element

def run():
   reload(sys)
   sys.setdefaultencoding('utf8')

   # Command line arguments
   parser = argparse.ArgumentParser(description='Polimorf Transform')
   parser.add_argument('--input', required=True, help='Specify Local or Cloud Storage for input files')
   parser.add_argument('--output', required=True, help='Specify Local or Cloud Storage Path for output files')
   parser.add_argument('--bucket', required=False, help='Specify Cloud Storage Bucket for output', default='none')
   parser.add_argument('--project',required=False, help='Specify Google Cloud Project', default='none')
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

   output_prefix = 'polimorf.output'
   output_file_suffix = '.csv'

   # Let's get our dictionary shaped according to our needs
   (p
       | 'ReadFromText' >> beam.io.ReadFromText(input)
       | 'AddMissingData' >> beam.ParDo(AddTypeOfWord())
       | 'ExtractData' >> beam.ParDo(TransformPolimorf())
       | 'RemveNotNouns' >> beam.ParDo(FilterNouns())
       | 'WriteToText' >> beam.io.WriteToText(file_path_prefix = '{}\{}'.format(output, output_prefix), file_name_suffix = output_file_suffix)
   )

if __name__ == '__main__':
   run()
